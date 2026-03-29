# 企业微信渠道插件

这个插件包用于把企业微信接入到知微统一渠道控制面。

## 当前状态

- 已具备可安装的 Marketplace 插件 manifest
- 已具备 schema、README、图标和示例配置
- 运行时模型为外部 HTTP connector
- 实际 connector 服务还需要单独开发

## 配置字段

- `baseUrl`：企业微信 connector 的 HTTP 基础地址
- `corpId`：企业 ID
- `agentId`：应用 Agent ID
- `token`：消息回调 Token
- `encodingAesKey`：消息加解密 AES Key

## 接入说明

企业微信通常以 webhook 回调方式接入，因此 connector 需要负责：

- URL 验证
- 回调解密
- 消息事件解析
- 主动发送消息

知微主服务只处理统一 runtime 事件，不再直接实现企微协议。

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
