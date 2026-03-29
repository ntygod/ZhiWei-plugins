# 钉钉渠道插件

这个插件包用于把钉钉接入到知微统一渠道控制面。

## 当前状态

- 已具备可安装的 Marketplace 插件 manifest
- 已具备 schema、README、图标和示例配置
- 运行时模型为外部 HTTP connector
- 实际 connector 服务还未完成

## 配置字段

- `baseUrl`：钉钉 connector 的 HTTP 基础地址
- `clientId`：应用 Client ID
- `clientSecret`：应用 Client Secret
- `robotCode`：企业机器人场景可选

## 接入说明

钉钉 connector 需要负责：

- 回调事件接收
- 鉴权和签名校验
- 消息事件标准化
- 主动消息发送

知微主服务只接收统一 runtime 事件，不直接承载钉钉平台协议。

## 运行时约定

connector 需要实现：

- `POST /instances/{instanceId}/start`
- `POST /instances/{instanceId}/stop`
- `POST /instances/{instanceId}/reload`
- `GET /instances/{instanceId}/health`
- `POST /instances/{instanceId}/deliver`

并向知微回调：

- `POST /api/channel-runtime/instances/{instanceId}/events`
- `POST /api/channel-runtime/instances/{instanceId}/heartbeat`
