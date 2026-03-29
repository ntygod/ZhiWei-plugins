# Channel Connectors

这个目录存放知微官方渠道插件对应的外部 connector 工程。

当前包含：

- `mock-http-connector`

后续建议新增：

- `feishu-connector`
- `wecom-connector`
- `dingtalk-connector`

每个 connector 至少应实现：

- `POST /instances/{instanceId}/start`
- `POST /instances/{instanceId}/stop`
- `POST /instances/{instanceId}/reload`
- `GET /instances/{instanceId}/health`
- `POST /instances/{instanceId}/deliver`

如需主动回调主服务，还应调用：

- `POST /api/channel-runtime/instances/{instanceId}/events`
- `POST /api/channel-runtime/instances/{instanceId}/heartbeat`
