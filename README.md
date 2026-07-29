# platform-integration

`platform-integration` 是 iFactory 平台的独立集成服务骨架。本阶段仅提供健康检查和
受控的 `serve` 运行角色；尚未实现业务集成链路。

## 本地运行

```bash
uv sync --frozen
uv run platform-integration serve --host 0.0.0.0 --port 8080
```

服务健康检查位于 `GET /healthz`，成功时返回 `{"status":"ok"}`。

可通过 `PLATFORM_INTEGRATION_PDM_CREDENTIAL_REF` 提供外部凭据引用；服务不会保存、
合成或打印凭据值。

## 验证

```bash
uv run pytest
uv run ruff check .
uv run ruff format --check .
```
