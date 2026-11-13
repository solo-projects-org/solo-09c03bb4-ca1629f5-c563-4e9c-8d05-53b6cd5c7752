# 种子发芽试验质控采集库

种子质控数据员把检测机构发布的发芽试验结果采集进质控库的完整系统：
**FastAPI + SQLite**，含本地检测机构 OAuth2 模拟源、可操作网页、两个独立采集进程。

## 业务与安全约束（本系统的核心）

两个独立采集进程（`collector-a` / `collector-b`）**共用一个 OAuth2 账户**，
访问令牌短期有效，刷新令牌每次刷新立即轮换作废旧令牌。系统保证：

| 约束 | 实现机制 |
| --- | --- |
| 多个请求遇到同一过期令牌，只刷新一次 | 进程内 `asyncio.Lock` + 跨进程 SQLite `BEGIN IMMEDIATE` 抢占 `refresh_state` 行（单飞 single-flight），其它进程轮询等待 |
| 迟到的 401 不能废弃新令牌 | 401 响应回显令牌 `jti`，与库中当前 `jti` 不一致即判定迟到，只重试不刷新 |
| 一个进程等慢报告，另一个仍能取数 | 每个采集进程多个并发槽 + 完全异步 HTTP，慢请求不阻塞其它槽位/进程 |
| 刷新结果无法确认（含刷新进程中途退出）→ 暂停账户并提示重新授权 | 网络/5xx 归为 ambiguous 错误；刷新认领带租约，租约超时未完成即判定进程崩溃，二者都把账户置 `paused` |
| 等待刷新时仍可重新授权，迟到刷新不得覆盖新授权 | `auth_epoch` 授权纪元：重新授权纪元 +1 并清空刷新状态；迟到刷新结果落盘时纪元不匹配则丢弃（`ReauthSuperseded`） |
| 凭据与结果存入数据卷，保存失败不报成功 | 全部落 `/data/qc.db`（Docker 卷）；结果与任务状态同一事务提交，回滚则任务保持未完成 |

## 目录结构

```
app/                  质控库应用
  config.py           环境变量配置
  db.py               SQLite 持久层（WAL、IMMEDIATE 事务、事件日志）
  asyncio_util.py     独立 DB 线程池（避免忙轮询饿死 web 回调）
  lab_client.py       检测机构 OAuth2 / 报告 API 客户端（区分确定失败与 ambiguous）
  tokens.py           令牌生命周期协调：单飞刷新、迟到401、租约熔断、授权纪元
  collector.py        采集任务队列与结果落库
  worker.py           独立采集进程（compose 中部署两个）
  web.py              网页 / API
  templates/          可操作网页
mock_lab/
  server.py           本地检测机构模拟源（OAuth2 授权服务器 + 报告 API + 演示控制）
scripts/
  entrypoint.sh       容器入口（mock / web / worker 三种角色）
  run_local.sh        无 Docker 时本地一键启动
  demo_test.py        端到端自测：8 个场景 34 项断言
docker-compose.yml    一键编排
Dockerfile
```

## 方式一：Docker Compose 启动（推荐演示）

```bash
docker compose up --build
```

- 网页（质控库）：http://localhost:8080
- 检测机构模拟源（含 /docs、/debug/state）：http://localhost:9000
- SQLite 数据库与全部凭据保存在命名卷 `qc-data`（容器内 `/data/qc.db`）

操作流程：
1. 打开 http://localhost:8080，点「发起授权 / 重新授权」，在模拟源页面点「同意授权」；
2. 在「提交报告号」输入如 `GR-2026-0001` 提交，两个采集进程自动采集；
3. 在「质控库结果」查看样本号、应测粒数、发芽粒数；「事件日志」观察协调过程。

## 方式二：本地直接启动（无 Docker）

```bash
pip install -r requirements.txt
./scripts/run_local.sh
# 同样访问 http://127.0.0.1:8080
```

## 三个关键演示

### 1. 刷新令牌轮换 + 单飞
网页「演示控制台」把访问令牌寿命设为 5~8 秒，再一次提交多个报告号。
事件日志里只会出现 **一次** `refresh_claim` / `refresh_ok`，其它参与者记录 `refresh_wait`；
模拟源 `/debug/state` 显示旧刷新令牌进入 revoked、当前只有一个有效刷新令牌。

### 2. 迟到 401 不废新令牌
在「迟到 401」框对 `GR-DEMO-401` 点「注入401」后提交该报告号。
模拟源只对首次请求回一个带**旧 jti** 的 401；系统记录 `late_401_ignored`，
直接用当前新令牌重试成功，当前令牌 jti 不变、不触发刷新。

### 3. 刷新中断 → 暂停 → 重新授权
- **结果无法确认**：「刷新故障」选 `ambiguous` 并应用，再作废旧令牌
  （或等短寿命令牌过期），账户立即 `paused`，页面显示重新授权提示，任务滞留队列；
  解除故障后点「重新授权」，滞留任务自动采集成功。
- **刷新进程中途退出**：
  ```bash
  # 给某个采集进程加上“刷新成功后、落盘前退出”的演示钩子后重启它
  docker compose run -e CRASH_AFTER_REFRESH_HTTP=collector-a --rm collector-a
  ```
  该进程在刷新中途退出后，其它进程在租约（`REFRESH_LEASE_SECONDS`，默认 15s）超时后
  记录 `refresh_lease_timeout` 并暂停账户；重新授权后恢复。

### 附加：慢报告不阻塞
对 `GR-DEMO-SLOW` 设为慢报告（如 6 秒），同时提交它和普通报告号，
普通报告 1 秒左右即完成，证明一个进程等待时另一个进程/槽位照常取数。

## 端到端自测

```bash
pip install -r requirements.txt
python3 scripts/demo_test.py
```

自动启动模拟源、web、两个采集进程，覆盖 8 个场景、34 项断言：
基础采集、单飞刷新与轮换、迟到 401、ambiguous 暂停与恢复、刷新进程崩溃租约熔断、
授权纪元防护、慢报告并发、保存失败不报成功。

## 关键环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `QC_DB_PATH` | `data/qc.db` | SQLite 库路径（容器内 `/data/qc.db`，挂数据卷） |
| `MOCK_BASE_URL` | `http://127.0.0.1:9000` | 检测机构模拟源地址 |
| `REFRESH_LEASE_SECONDS` | 15 | 刷新租约，超时判定刷新进程中途退出 |
| `TOKEN_EXPIRY_MARGIN` | 5 | 访问令牌提前判定过期的余量（秒） |
| `WORKER_SLOTS` | 2 | 每个采集进程的并发采集槽数 |
| `JOB_STALE_SECONDS` | 60 | running 任务超时回收（进程崩溃遗留） |
| `CRASH_AFTER_REFRESH_HTTP` | 空 | 演示钩子：指定 worker 名在刷新后落盘前退出 |
