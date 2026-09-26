# 本地多上游 API 网关

基于 FastAPI 的本地可运行网关层。仓库内以 `MockUpstream` 模拟上游服务，无外部依赖，
覆盖路由与版本协商、限流与配额、熔断与故障切换，全部错误归一化为可区分的类别。

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

## 路由与版本规则

- 路由按 `方法 + 路径模板` 注册，模板支持 `{param}` 路径参数（如 `/items/{item_id}`），
  按注册顺序首个命中生效；未命中返回 `route_not_found`（HTTP 404）。
- 每条路由携带若干 `RouteVersion`，每个版本有自己的上游列表（主备有序）。
- 版本协商：在「调用方可接受版本 ∩ 路由提供版本」中取**最高版本**，结果确定；
  交集为空返回 `version_not_satisfied`（HTTP 406），消息中列出双方版本集合。

## 限流与配额语义

限流维度为 `(调用方, 路由)`，无规则的维度默认放行。`RateLimitRule` 三个参数：

| 参数 | 含义 |
| --- | --- |
| `burst_limit` | 每个 `window_seconds` 固定窗口内允许的最大请求数（突发上限） |
| `window_seconds` | 突发计数窗口长度（秒） |
| `quota` | 该维度累计允许的最大请求数（长期配额） |

- 超限拒绝原因可区分：突发超限 → `rate_limited`（可重试）；配额耗尽 → `quota_exhausted`（不可重试）。两者同时触发时优先报告配额耗尽，判定确定。
- 「检查 + 递增」在同一把锁内完成，高并发下计数原子、不超发（见并发测试）。
- `update_rules` 以不可变快照整体替换规则：读取方要么看到旧快照、要么看到新快照，
  不存在新旧配置混合生效的中间状态。

## 熔断与故障切换

熔断器按上游实例维护，参数：`failure_threshold`（连续失败熔断阈值）、
`recovery_seconds`（熔断后进入半开的等待时长）、`half_open_max_probes`（半开期间允许的并发探测数）。

状态迁移：`CLOSED →（连续失败达阈值）→ OPEN →（等待 recovery_seconds）→ HALF_OPEN →（探测成功）→ CLOSED`；
半开探测失败立即回到 `OPEN` 并重新计时。半开期间仅放行受限数量的探测请求，其余返回 `circuit_open`。

故障切换规则（`Gateway.handle`）：

- 熔断拒绝意味着请求**未发出**，任何请求都可安全尝试下一上游；
- 超时 / 不可用意味着请求**可能已执行**，仅幂等请求（GET/PUT/DELETE）允许切换备用上游，
  非幂等请求（POST）直接失败，绝不重复执行；
- 上游业务错误（4xx/5xx）说明上游已应答，不做切换，直接归一化返回；
- 上游 5xx 计入熔断失败，4xx 视为上游健康。

## 错误分类

所有底层异常在网关边界归一化为 `GatewayError`，不向调用方透出：

| category | HTTP | 含义 | 可重试 |
| --- | --- | --- | --- |
| `route_not_found` | 404 | 无匹配路由 | 否 |
| `version_not_satisfied` | 406 | 版本协商失败 | 否 |
| `rate_limited` | 429 | 突发超限 | 是 |
| `quota_exhausted` | 429 | 配额耗尽 | 否 |
| `circuit_open` | 503 | 熔断/半开拒绝 | 是 |
| `upstream_timeout` | 504 | 上游超时 | 是 |
| `upstream_unavailable` | 503 | 上游不可用 | 是 |
| `upstream_error` | 502 | 上游业务错误 | 否 |

## 测试与可控时钟

- 时间相关行为（限流窗口、熔断恢复、上游延迟）全部通过注入的 `Clock` 驱动；
  测试使用 `ManualClock.advance()` 确定性推进，无需真实睡眠，结果可复现。
- `MockUpstream` 用 `Behavior` 脚本化每次调用的表现（ok/timeout/unavailable/error + 模拟延迟），
  延迟通过推进共享时钟体现；`call_count` 用于断言非幂等请求未被重复执行。
- 覆盖：并发限流原子性、幂等/非幂等故障切换、熔断状态迁移（含半开探测成功/失败）、
  版本协商失败、HTTP 层错误映射。测试日志（`log_cli`）打印关键输入与判定依据。

## 代码结构

```
gateway/
  clock.py      可控时钟（SystemClock / ManualClock）
  errors.py     错误类别与 GatewayError
  models.py     路由、版本、上游、请求/响应模型
  router.py     路径模板匹配与版本协商
  ratelimit.py  原子限流与配额（快照式规则替换）
  circuit.py    熔断状态机
  upstreams.py  可脚本化的模拟上游
  gateway.py    编排：路由→限流→熔断→调用→故障切换→错误归一化
  app.py        FastAPI 暴露层与演示装配
main.py         本地运行入口
tests/          单元与集成测试
```
