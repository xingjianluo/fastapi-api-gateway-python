# Multi-Upstream API Gateway（本地可运行）

一个不依赖任何外部服务的多上游 API 网关：路由与版本协商、按调用方/路由维度的
限流与配额、熔断器与故障切换全部由仓库内的**模拟上游**驱动，因此路由、限流、
故障切换在并发与异常条件下的行为都可复现、可解释。

- 路由：路径模板（含 `{path_param}`）+ HTTP 方法，确定性的模板优先级
- 版本：按调用方 `X-Accept-Version` 可接受范围，从路由已发布版本中确定性选择
- 故障切换：仅幂等方法允许在**不可用 / 超时 / 熔断打开**时切换备用上游；
  非幂等请求绝不重复执行；上游业务错误（即使 5xx）也不重试
- 限流：令牌桶（突发）+ 周期配额（长周期），三类拒绝原因可区分；
  规则整表原子替换，不出现新旧配置混合生效
- 熔断：`closed → open → half-open → closed/open`，半开期间仅放行受限探测流量
- 错误：底层异常一律归一化为固定错误码，绝不透出 traceback
- 测试：并发限流、幂等/非幂等故障切换、熔断迁移、版本协商失败均有确定性测试，
  时间相关行为全部通过可控时钟 `FakeClock` 推进

## 目录结构

```
gateway/
  clock.py         # Clock 协议、SystemClock、可推进的 FakeClock
  errors.py        # 归一化错误码与异常类型
  config.py        # 路由 / 上游 / 限流 / 熔断配置模型
  versioning.py    # 语义化版本解析与确定性协商
  router.py        # 路径模板匹配与版本选择
  ratelimiter.py   # 原子令牌桶 + 窗口配额
  breaker.py       # 每个上游的熔断器状态机
  upstream.py      # 仓库内模拟上游（ok/fail/timeout/busy/script）
  gateway.py       # 编排：路由→限流→熔断→故障切换→错误归一化
  app.py           # FastAPI HTTP 层
main.py            # 本地演示配置与启动入口
tests/             # 55 个确定性测试
```

## 快速开始

```bash
uv sync                       # 安装依赖（含 pytest）
uv run python main.py         # 启动演示网关 http://127.0.0.1:8000
```

另开终端：

```bash
# 幂等 GET：主上游故障 -> 自动切换备用上游（x-retried: 1）
curl -i -H 'X-Caller-Id: demo' -H 'X-Accept-Version: 2' \
  http://127.0.0.1:8000/orders/123

# 非幂等 POST：主上游超时 -> 504，请求绝不重复发到备用上游
curl -i -X POST -H 'X-Caller-Id: demo' http://127.0.0.1:8000/payments

# 版本无法满足 -> 400 version_not_acceptable，details 带可接受范围与在服版本
curl -i -H 'X-Accept-Version: 9' http://127.0.0.1:8000/orders/1

# 突发流量：该路由容量 5，连续第 6、7 个请求 -> 429 rate_limited
for i in $(seq 1 7); do
  curl -s -o /dev/null -w '%{http_code} ' -H 'X-Caller-Id: burst' \
    http://127.0.0.1:8000/orders/1
done; echo
```

## 请求约定

| Header | 含义 |
| --- | --- |
| `X-Caller-Id` | 调用方标识，限流/配额按 `(caller, route)` 维度计数；缺省为 `anonymous` |
| `X-Accept-Version` | 调用方可接受的版本范围；缺省表示"接受最高在服版本" |
| `X-Request-Cost` | 本次请求在令牌桶中的权重，缺省 `1.0` |

响应头：`X-Served-By`（实际服务的上游）、`X-Api-Version`（协商出的版本）、
`X-Retried`（是否经过故障切换）、失败时 `X-Error-Code`（归一化错误码）。

## 路由与版本规则

### 路由匹配

- 路径模板以 `/` 开头，支持 `{name}` 形式的路径参数，如 `/orders/{order_id}`。
- 匹配顺序是**确定性**的：按分段比较，字面量分段优先于参数分段；
  优先级相同则按配置中的注册顺序。因此 `/orders/special` 永远优先于
  `/orders/{order_id}`。
- 末尾斜杠归一化（`/orders/1/` 等价于 `/orders/1`）。
- 路径无匹配 → `route_not_found`(404)；路径匹配但方法不允许 → 同样
  `route_not_found`，但 `details.allowed_methods` 给出允许的方法，可区分。

### 版本协商

每个路由声明一组已发布版本（如 `("1.0.0", "2.0.0", "2.1.0")`）。
`X-Accept-Version` 支持：

| 写法 | 语义 |
| --- | --- |
| 缺省 / `*` | 最高在服版本 |
| `1` / `1.2` | 该主版本线内、不低于给定版本 |
| `1.2.3` | 精确该版本 |
| `>=1.2.0,<2.0.0` | 逗号分隔的比较列表，取交集 |
| `^1.2.3` | 兼容版本：`>=1.2.3,<2.0.0` |
| `~1.2.3` | 次版本锁定：`>=1.2.3,<1.3.0` |

选择规则：在可接受范围内的在服版本中**取最高者**，同一输入永远得到同一结果。
没有任何在服版本满足时返回 `version_not_acceptable`(400)，错误详情同时带
`requested`（可接受范围）与 `available`（路由在服版本），便于调用方纠偏。

## 限流与配额配置语义

每条路由可配置一个 `RateLimitRule`：

```python
RateLimitRule(
    capacity=10.0,      # 令牌桶容量：允许的最大突发
    refill_rate=5.0,    # 每秒补充的令牌数
    quota_limit=50,     # 可选：每个 quota_window 内最多接受的请求数
    quota_window=60.0,  # 配额窗口（秒），窗口滑动重置
)
```

- 计数维度：`(X-Caller-Id, route)`，不同调用方/不同路由互不影响。
- 原子性：每个键的"补充→判定→扣减"在同一把锁内完成；高并发下允许通过的
  请求数严格等于桶容量，测试用 20 线程屏障并发验证（10 放行 / 10 拒绝）。
- 三类拒绝原因彼此可区分：
  - `rate_limited`(429)：突发令牌暂时不足，`details.retry_after_seconds`
    给出最早可重试时间，时钟推进后会自动补充；
  - `scope_insufficient`(429)：单个请求的 `cost` 大于桶容量，永远无法满足，
    且**不消耗**任何令牌；
  - `quota_exhausted`(429)：长周期配额耗尽，`details.reset_in_seconds`
    给出窗口重置剩余时间。
- 规则更新：`Gateway.update_config()` 在同一临界区内同时替换路由表与限流
  规则表，限流计数按新规则重新起算；进行中的请求持有旧快照并按旧规则执行
  到底，因此**不会出现一次请求半新半旧**。并发测试断言高并发换规则期间
  每个决策只可能来自某一个完整版本的配置。

## 熔断配置语义

全局 `CircuitBreakerConfig`：

```python
CircuitBreakerConfig(
    failure_threshold=3,     # CLOSED 下连续失败达到该值 -> OPEN
    cooldown_seconds=30.0,   # OPEN 持续时间，过后首个请求转入 HALF_OPEN 探测
    half_open_max_probe=1,   # HALF_OPEN 期间最多同时放行的探测请求数
    probe_success_threshold=1,  # 连续探测成功多少次后回到 CLOSED
)
```

- 每个上游一个独立熔断器；连续失败计数只统计失败、成功即清零。
- `OPEN` 期间所有发往该上游的请求直接以 `upstream_circuit_open`(503) 拒绝，
  `details.retry_after_seconds` 告知冷却剩余时间。
- 冷却结束后**第一个**请求把熔断器确定性地带入 `HALF_OPEN` 并成为探测请求；
  超出 `half_open_max_probe` 的并发请求仍被拒绝（并发测试：5 并发、上限 2，
  恰好 2 放行 / 3 拒绝）。
- 探测判定稳定：任何一次探测失败立即重新 `OPEN` 并重置冷却计时；
  连续 `probe_success_threshold` 次成功才回到 `CLOSED` 并清零失败计数。
- 时间全部走注入的时钟：测试用 `FakeClock.advance(seconds)` 推进冷却，
  不依赖真实 sleep，结果毫秒级、可复现。

## 故障切换与幂等性

路由按顺序配置一个或多个上游，网关逐个尝试，并在每次尝试前咨询熔断器：

- 允许切换的失败类别：`upstream_unavailable`（连接类失败）、
  `upstream_timeout`（超时）、`upstream_circuit_open`（熔断打开）。
- **仅幂等方法**（`GET/HEAD/OPTIONS/PUT/DELETE`）允许尝试下一个上游；
  `POST/PATCH` 等非幂等请求只执行一次，主上游失败即返回归一化错误。
- 上游业务错误（`busy`，即使 HTTP 5xx）代表上游给出了**确定性应答**，
  不属于"处置未知"，任何方法都不会重试。
- 所有上游都不可用时返回最后一个上游的归一化错误原因。
- 模拟上游是进程内对象并记录调用次数，测试直接断言
  "非幂等失败时备用上游 `call_count == 0`"。

## 错误分类（底层异常不透出）

| 错误码 | HTTP | 含义 |
| --- | --- | --- |
| `route_not_found` | 404 | 路径无路由，或方法不允许（看 `details.allowed_methods`） |
| `version_not_acceptable` | 400 | 版本协商失败（`details.requested` / `available`） |
| `rate_limited` | 429 | 突发限流，稍后可重试（`retry_after_seconds`） |
| `scope_insufficient` | 429 | 单请求权重超过桶容量，重试无意义 |
| `quota_exhausted` | 429 | 周期配额耗尽（`reset_in_seconds`） |
| `upstream_circuit_open` | 503 | 熔断器打开 / 半开探测名额已满 |
| `upstream_timeout` | 504 | 上游在预算时间内未完成 |
| `upstream_unavailable` | 502 | 上游连接类失败（拒绝/重置等） |
| `upstream_business_error` | 502 | 上游给出业务错误应答（不重试） |
| `internal_error` | 500 | 兜底：任何未预期异常也只输出此结构 |

统一响应体：

```json
{"error": {"code": "rate_limited", "message": "...", "details": {...}}}
```

## 模拟上游说明

`UpstreamConfig(kind=...)`：`ok`（成功，可带 `delay`/`status`/`body`）、
`fail`（连接失败）、`timeout`（耗时超过请求预算）、`busy`（业务错误）、
`script`（按 `sequence` 顺序给出结果，末项循环，用于构造"先失败后恢复"）。

时间双模式：测试使用 `FakeClock`，超时按"声明耗时 vs 剩余预算"即时判定，
不真实等待；本地 HTTP 服务使用 `SystemClock`，会真实 sleep 到超时。

## 本地验证

```bash
uv run python -m pytest          # 全部 55 个测试
uv run python -m pytest -q       # 静默模式；判定依据见 -s / 实时日志
```

测试实时打印关键输入与判定依据（`pyproject.toml` 中配置了 `log_cli`），例如：

```
INFO gateway | route resolved | caller=alice method=GET path=/orders/42 version_header='2' -> route=/orders/{order_id} version=2.0.0 ...
INFO gateway | upstream failure | upstream=orders-primary attempt=0 idempotent=True code=upstream_unavailable retryable=True
INFO gateway | upstream success | upstream=orders-backup attempt=1 status=200
```

重点覆盖：

- `tests/test_ratelimiter.py`：20 线程并发突发计数严格等于容量、令牌补充、
  scope 不足、配额耗尽与重置、按 caller/route 独立、换规则原子性（含并发换规则）；
- `tests/test_failover.py`：幂等 GET 故障切换、非幂等 POST 不重复执行、
  超时可切换、业务错误不重试、上游全失败、脚本化恢复；
- `tests/test_breaker.py`：closed→open、冷却后 half-open 探测成功/失败、
  半开探测名额并发限制、多次成功阈值、网关级熔断后恢复；
- `tests/test_versioning.py` / `tests/test_router.py`：版本协商矩阵、
  协商失败可区分、模板优先级与路径参数；
- `tests/test_http.py`：FastAPI TestClient 端到端验证响应头与错误码；
- `tests/test_gateway_integration.py`：并发换配置无混合状态、错误归一化。
