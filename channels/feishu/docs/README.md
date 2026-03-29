# 飞书渠道插件

这个插件包用于把飞书接入到知微统一渠道控制面。

## 当前状态

- 已具备可安装的 Marketplace 插件 manifest
- 已具备控制面 schema、README 和示例配置
- 运行时默认设计为外部 HTTP connector
- 实际 connector 镜像仍需单独实现和发布

## 配置字段

- `baseUrl`：飞书 connector 的 HTTP 基础地址
- `appId`：飞书应用 App ID
- `appSecret`：飞书应用 App Secret
- `connectionMode`：`websocket` 或 `webhook`
- `verificationToken`：仅 webhook 模式需要
- `encryptKey`：仅 webhook 模式需要

## 推荐接入方式

默认优先使用 `websocket` 模式，这样不需要把知微主服务直接暴露到公网。

如果必须走公网回调，再切换到 `webhook` 模式，并补齐：

- `verificationToken`
- `encryptKey`

## 运行时约定

connector 应实现以下接口：

- `POST /instances/{instanceId}/start`
- `POST /instances/{instanceId}/stop`
- `POST /instances/{instanceId}/reload`
- `GET /instances/{instanceId}/health`
- `POST /instances/{instanceId}/deliver`

同时向知微主服务回调：

- `POST /api/channel-runtime/instances/{instanceId}/events`
- `POST /api/channel-runtime/instances/{instanceId}/heartbeat`

## 下一步

这个插件包已经可以作为 Marketplace 发布单元存在，但还不是完整的飞书 connector 实现。
