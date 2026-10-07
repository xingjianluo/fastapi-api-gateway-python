# 本地多上游 API 网关

基于 FastAPI 的本地可运行网关层。仓库内以 `MockUpstream` 模拟上游服务，无外部依赖，
覆盖路由与版本协商、按权重的灰度分流、限流与配额、上游共享容量保护、熔断与故障切换、
灰度与健康保护联动，全部错误归一化为可区分的类别。

## 运行与验证

```bash
uv sync                  # 安装依赖
uv run pytest            # 运行全部测试（日志打印关键输入与判定依据）
uv run python main.py    # 本地启动网关，监听 127.0.0.1:8000
```

调用示例：

```bash
curl -H 'X-Caller-Id: demo' -H 'X-Accept-Versions: 1,2' http://127.0.0.1:8000/items/1
# => {"upstream":"items-v2a","version":2,"body":{"svc":"v2a"}}
```

请求头语义：`X-Caller-Id` 声明调用方（缺省 `anonymous`）；
`X-Accept-Versions` 为逗号分隔的可接受版本（缺省接受任意版本）。

### 运行期管理端点（本地验证灰度与容量）

```bash
# 整体替换 get-item 的版本权重：v1=100, v2=0（v2 立即摘流，新请求全部落 v1）
curl -X PUT -H 'Content-Type: application/json' \
  -d '{"weights":{"1":100,"2":0}}' http://127.0.0.1:8000/-/admin/weights/get-item

# 调整上游共享容量与调用方预留
curl -X PUT -H 'Content-Type: application/json' \
  -d '{"capacity":50,"reserved":{"vip":10}}' http://127.0.0.1:8000/-/admin/capacity/items-v2a
```

调整对本进程立即生效（快照语义）；已放行的在途请求按进入时的判定走完。

## 路由与版本规则

- 路由按 `方法 + 路径模板` 注册，模板支持 `{param}` 路径参数（如 `/items/{item_id}`），
  按注册顺序首个命中生效；未命中返回 `route_not_found`（HTTP 404）。
- 每条路由携带若干 `RouteVersion`，每个版本有自己的上游列表（主备有序）。
- 未配置权重表时保持原协商行为：在「调用方可接受版本 ∩ 路由提供版本」中取**最高版本**；
  交集为空返回 `version_not_satisfied`（HTTP 406）。

## 灰度权重与调用方粘性

为某条路由配置权重表后（`VersionWeighter.update_weights(route_id, {版本: 权重})`），
新请求在**可接受版本**内按有效权重分流：

- **确定性分桶**：把 `(route_id, caller_id)` 用 SHA-256 哈希到 `[0,1)` 的固定点，
  再按版本号升序累加权重的分桶选择。不依赖随机数，跨进程、跨次运行结果一致。
- **调用方粘性**：权重快照不变时，同一调用方在同一路由上**永远落同一版本**，
  幂等重试与故障切换判定前后一致；权重调整后新请求按新快照重新分桶，
  已在处理中的请求按进来时的判定走完，不存在半新半旧的中间状态。
- **运行期调整**：`update_weights` 以不可变快照整体替换该路由权重，新请求立即生效。
- **权重 0 摘流**：权重为 0 的版本不接新请求；全部可接受版本有效权重均为 0 时
  返回 `version_not_satisfied`。

## 上游共享容量与优先级

`UpstreamCapacityManager` 在每个上游实例上维护所有调用方共享的并发容量
（`CapacityRule(capacity, reserved={调用方: 预留数})`）：

- **硬上限**：该上游在途请求总数达到 `capacity` 后，任何新请求都被拒绝；
  「检查 + 占用」在同一把锁内完成，并发下不超发。
- **预留（优先级）**：调用方在途数小于自身预留额度时直接放行（使用自己的预留槽）。
- **借用与归还**：预留未用满的部分允许其他调用方临时借用——借用方的可用空间为
  `公共池(capacity - 全部预留) + 其他调用方当前未使用的预留`。高优先级回来占用预留后，
  可借用空间立即收缩，低优先级的**新请求**被拒；在途请求绝不中途掐断，
  额度随其完成自然归还。
- **快照替换**：`update_rules` 原子替换全部规则；在途计数独立于规则快照，
  换规则不混账。无规则的上游不限制（仍记账便于观测）。
- 容量拒绝与熔断拒绝一样意味着**请求未发出**，网关在主备上游间安全故障切换；
  占用与释放在 `finally` 中成对完成。

## 灰度与健康保护联动

`CanaryController` 以版本首选上游的熔断状态为健康信号，按 `(路由, 版本)` 维护状态机
（时间由注入时钟驱动，同样输入序列结果可复现）：

```
NORMAL ──首选上游熔断 OPEN──> PROTECTED ──保护满 protect_seconds──> RECOVERING
   ^                                                              │    │
   └────────────── 所有恢复阶段完成 ──────────────────────────────┘    │
            PROTECTED <──恢复期间再次熔断（探测失败，立即退回并重新计时）─┘
```

- **PROTECTED**：该版本有效权重压到 0，新流量全部回到其余（稳定）版本，
  不再往坏的上游灌流量。
- **RECOVERING**：按 `recovery_steps`（如 `(0.25, 0.5, 1.0)`）分阶段把权重加回配置值，
  每阶段至少持续 `step_seconds`，只放少量试探流量；试探流量同时充当熔断器
  半开探测的来源（避免权重为 0 时熔断器永远等不到探测的死锁，
  因此 `protect_seconds` 应 >= 熔断器 `recovery_seconds`）。
- **不提前放开**：阶段推进只看时钟与熔断状态，单个迟到的成功结果不会提前提权；
  恢复期间探测失败立即退回 PROTECTED 并重新计保护期。

## 限流与配额语义

限流维度为 `(调用方, 路由)`，无规则的维度默认放行。`RateLimitRule` 三个参数：

| 参数 | 含义 |
| --- | --- |
| `burst_limit` | 每个 `window_seconds` 固定窗口内允许的最大请求数（突发上限） |
| `window_seconds` | 突发计数窗口长度（秒） |
| `quota` | 该维度累计允许的最大请求数（长期配额） |

- 超限拒绝原因可区分：突发超限 → `rate_limited`（可重试）；配额耗尽 → `quota_exhausted`（不可重试）。两者同时触发时优先报告配额耗尽，判定确定。
- 「检查 + 递增」在同一把锁内完成，高并发下计数原子、不超发（见并发测试）。
- `update_rules` 以不可变快照整体替换规则：读取方要么看到旧快照、要么看到新快照。

## 熔断与故障切换

熔断器按上游实例维护，参数：`failure_threshold`（连续失败熔断阈值）、
`recovery_seconds`（熔断后进入半开的等待时长）、`half_open_max_probes`（半开期间允许的并发探测数）。

状态迁移：`CLOSED →（连续失败达阈值）→ OPEN →（等待 recovery_seconds）→ HALF_OPEN →（探测成功）→ CLOSED`；
半开探测失败立即回到 `OPEN` 并重新计时。半开期间仅放行受限数量的探测请求，其余返回 `circuit_open`。

故障切换规则（`Gateway.handle`）：

- 熔断拒绝与**容量拒绝**意味着请求**未发出**，任何请求都可安全尝试下一上游；
- 超时 / 不可用意味着请求**可能已执行**，仅幂等请求（GET/PUT/DELETE）允许切换备用上游，
  非幂等请求（POST）直接失败，绝不重复执行；
- 上游业务错误（4xx/5xx）说明上游已应答，不做切换，直接归一化返回；
- 上游 5xx 计入熔断失败，4xx 视为上游健康。

## 错误分类

所有底层异常在网关边界归一化为 `GatewayError`，不向调用方透出：

| category | HTTP | 含义 | 可重试 |
| --- | --- | --- | --- |
| `route_not_found` | 404 | 无匹配路由 | 否 |
| `version_not_satisfied` | 406 | 版本协商失败 / 可接受版本有效权重全为 0 | 否 |
| `rate_limited` | 429 | 调用方突发超限 | 是 |
| `quota_exhausted` | 429 | 调用方配额耗尽 | 否 |
| `upstream_capacity_exceeded` | 503 | 上游共享容量占满（所有调用方共享的并发上限） | 是 |
| `circuit_open` | 503 | 熔断/半开拒绝 | 是 |
| `upstream_timeout` | 504 | 上游超时 | 是 |
| `upstream_unavailable` | 503 | 上游不可用 | 是 |
| `upstream_error` | 502 | 上游业务错误 | 否 |

三种「被拒绝」一眼可区分：`rate_limited`/`quota_exhausted` 是**调用方自己**超限，
`upstream_capacity_exceeded` 是**上游总容量**被所有调用方共享打满。

## 测试与可控时钟

- 时间相关行为（限流窗口、熔断恢复、灰度保护/恢复步进、上游延迟）全部通过注入的
  `Clock` 驱动；测试使用 `ManualClock.advance()` 确定性推进，无需真实睡眠，结果可复现。
- `MockUpstream` 用 `Behavior` 脚本化每次调用的表现（ok/timeout/unavailable/error + 模拟延迟），
  延迟通过推进共享时钟体现；`call_count` 用于断言非幂等请求未被重复执行、
  坏上游不再接收新流量。
- 覆盖：权重分流的确定性与调用方粘性、权重 0 摘流与运行期调整、共享容量并发原子性、
  优先级预留的借用与归还、灰度自动回退与分阶段恢复（含探测失败退回、
  恢复序列两次运行完全一致）、容量拒绝的故障切换与类别区分，
  以及既有的限流/熔断/故障切换/版本协商/HTTP 映射回归。
  测试日志（`log_cli`）打印关键输入与判定依据。

## 代码结构

```
gateway/
  clock.py      可控时钟（SystemClock / ManualClock）
  errors.py     错误类别与 GatewayError
  models.py     路由、版本、上游、请求/响应模型
  router.py     路径模板匹配与版本协商
  weights.py    灰度权重快照与调用方粘性分桶
  ratelimit.py  原子限流与配额（快照式规则替换）
  capacity.py   上游共享容量与优先级预留（借用/归还）
  circuit.py    熔断状态机
  canary.py     灰度与健康保护联动（自动回退、分阶段恢复）
  upstreams.py  可脚本化的模拟上游
  gateway.py    编排：路由→版本选择→限流→熔断→容量→调用→故障切换→错误归一化
  app.py        FastAPI 暴露层、运行期管理端点与演示装配
main.py         本地运行入口
```
