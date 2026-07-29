# platform-integration 协作约定

`platform-integration` 是平台内独立的集成服务。它通过稳定 HTTP API、事件和
`contracts/` 契约与其他组件交互；不得直连其他组件数据库、共享 ORM 模型或保存凭据。

## 开发

- 从总仓根目录确认组件状态和分支，再在本组件的短生命周期分支中修改业务代码。
- 运行 `uv sync --frozen`、相关 `pytest` 和 `ruff check .` 后才提交。
- 先推送组件提交，再由总仓单独记录 gitlink。

## 安全

- 不提交 `.env`、token、连接串、运行计划/回执、日志、训练数据、模型或构建缓存。
- 配置只接受外部凭据引用；不得内置或打印凭据值。
- 写操作必须通过稳定 API，并携带可审计的关联 ID 和幂等键。
