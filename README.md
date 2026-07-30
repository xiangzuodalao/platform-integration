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
```

- `migrate` 将本服务数据库升级到 Alembic head。
- `discover-identities` 只读发现 ThingsBoard 租户 ID 和 CMMS 公司 ID。
- `provision-plan`、`provision-apply` 和 `provision-verify` 提供计划、精确确认执行及
  持久回执流程；所有外部写入都通过稳定 API。
- `scheduler` 创建预测时隙，`prediction-worker` 领取并完成合资格运行，
  `shadow-summary` 只读输出有界验收证据。

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
