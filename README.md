# platform-integration

`platform-integration` 是 iFactory 平台的独立集成服务。它拥有租户/设备/测点绑定、
影子预测运行、内部风险状态和受控操作回执，通过稳定 API 协调 ThingsBoard、PDM 和
CMMS。

Phase 2 的预测链路只读取 ThingsBoard 历史遥测、调用 PDM 预测并保存有界证据和内部
风险状态；它不创建、更新、确认或清除 ThingsBoard Alarm，也不创建 CMMS 工单。服务
只访问自己的 PostgreSQL 数据库，不直连其他组件数据库、不共享 ORM 模型，也不保存或
打印凭据值。

## 本地运行

```bash
uv sync --frozen
uv run platform-integration serve --host 0.0.0.0 --port 8080
```

服务健康检查位于 `GET /healthz`，成功时返回 `{"status":"ok"}`。

## 命令角色

```text
platform-integration serve [--host HOST] [--port PORT]
platform-integration migrate
platform-integration discover-identities --format env
platform-integration provision-plan --tenant-alias ALIAS --actor ACTOR
platform-integration provision-apply --tenant-alias ALIAS \
  --plan-hash SHA256 --confirmed-hash SHA256 --actor ACTOR
platform-integration provision-verify --tenant-alias ALIAS --plan-hash SHA256
platform-integration scheduler [--once] [--now RFC3339]
platform-integration prediction-worker [--once] [--now RFC3339]
platform-integration shadow-summary --tenant-alias ALIAS \
  --scheduled-at RFC3339 --format json
platform-integration closed-loop-worker \
  --role {alarm,work-order,status-poll} --owner OWNER [--once] [--now RFC3339]
platform-integration closed-loop-summary --tenant-alias ALIAS --format json
platform-integration provider-readiness --role {alarm,work-order,status-poll}
platform-integration closed-loop-acceptance-verify --tenant-alias ALIAS \
  [--expected-stage {CONSISTENT,ACTIVE,IN_PROGRESS,COMPLETE,CLEARED}] --format json
```

- `migrate` 将本服务数据库升级到 Alembic head。
- `discover-identities` 只读发现 ThingsBoard 租户 ID 和 CMMS 公司 ID。
- `provision-plan`、`provision-apply` 和 `provision-verify` 提供计划、精确确认执行及
  持久回执流程；所有外部写入都通过稳定 API。
- `scheduler` 创建预测时隙，`prediction-worker` 领取并完成合资格运行，
`shadow-summary` 只读输出有界验收证据。

闭环默认由 `PLATFORM_INTEGRATION_CLOSED_LOOP_ENABLED=false` 禁用。隔离试点启用后，
`closed-loop-worker` 按角色交付 ThingsBoard Alarm、CMMS 工单创建或 CMMS 状态轮询；每个
进程只需注入该角色使用的凭据。`closed-loop-summary` 只输出按状态聚合的计数，不输出
凭据、原始遥测、完整预测数组或工单正文。

`provider-readiness` 是无副作用的凭据健康检查：Alarm 角色使用服务 Token 核对
ThingsBoard tenant，工单角色使用 CMMS Bearer 的 `/api/auth/me` 核对 company。凭据
过期、身份漂移或配置缺失均返回非零状态，输出不包含凭据值。

`closed-loop-acceptance-verify` 是隔离试点的只读验收命令。它从本服务数据库精确定位
选定设备当前告警聚合，要求租户 outbox 已结清且无 dead-letter，再经稳定 API 读回
ThingsBoard Alarm 和 CMMS 唯一 `external_ref` 工单并核对冻结身份、版本与状态。没有
可验收告警或指定阶段尚未到达时以 `CLOSED_LOOP_ACCEPTANCE_NOT_READY` 非零退出；输出只含
有界、无凭据的 canonical JSON 证据。未指定阶段时只检查一致性；`COMPLETE` 阶段要求
CMMS 工单已完成但风险和 Alarm 仍保持活动，以证明工单完成本身不会清除风险；`CLEARED`
阶段还要求连续健康计数至少为 2 且 ThingsBoard Alarm 已清除。

`--now` 是隔离验收专用时钟：只允许与 `--once` 同时使用，并且必须显式设置
`PLATFORM_INTEGRATION_ISOLATED_PILOT_MODE=1`。时间必须是严格 RFC3339，例如
`2026-07-29T01:15:00Z`。连续角色和生产模式拒绝时钟注入；未提供 `--now` 时使用
UTC 系统时钟。固定时隙验收应向 scheduler 和 prediction worker 传入同一个时间：

```bash
PLATFORM_INTEGRATION_ISOLATED_PILOT_MODE=1 \
  uv run platform-integration scheduler --once --now 2026-07-29T01:15:00Z
PLATFORM_INTEGRATION_ISOLATED_PILOT_MODE=1 \
  uv run platform-integration prediction-worker --once --now 2026-07-29T01:15:00Z
```

通过 `PLATFORM_INTEGRATION_*_CREDENTIAL_REF` 配置外部凭据引用；实际凭据由运行环境
按引用注入。

## 验证

不启动 Docker/testcontainers 的离线验证：

```bash
uv run pytest tests \
  --ignore=tests/integration \
  --ignore=tests/repositories \
  --ignore=tests/services/test_prediction_runs.py \
  --ignore=tests/test_container_hygiene.py \
  --ignore=tests/test_image_contract.py
uv run ruff check .
uv run ruff format --check .
```

完整 `uv run pytest` 还包含 PostgreSQL testcontainers 和 Docker 镜像契约测试，只在
明确具备并获准使用 Docker 的环境中运行。
