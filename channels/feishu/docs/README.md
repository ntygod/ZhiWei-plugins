# 飞书渠道插件

这个插件包用于把飞书接入到知微统一渠道控制面。

## 当前状态

- 已具备可安装的 Marketplace 插件 manifest
- 已具备控制面 schema、README 和示例配置
- 运行时默认设计为外部 HTTP connector
- 已有首个官方 `feishu-connector` 工程
- 安装包已附带可执行 `feishu-connector.jar`
- 当前 connector 已支持 `websocket` 和 `webhook` 两种模式
- 已支持飞书原生图片上传与发送
- 已支持飞书原生文件上传与发送
- 已支持飞书交互卡片动作回调

## 配置字段

- `baseUrl`：可选。用于覆盖主服务自动托管，改为连接自建飞书 connector
- `appId`：飞书应用 App ID
- `appSecret`：飞书应用 App Secret
- `connectionMode`：`websocket` 或 `webhook`
- `verificationToken`：仅 webhook 模式需要
- `encryptKey`：仅 webhook 模式需要

## 默认使用方式

官方插件默认会由知微主服务优先自动托管安装包内的 `feishu-connector` 运行时。在这种模式下，用户通常只需要：

- 安装飞书插件
- 创建实例
- 填写 `appId / appSecret`
- 选择 `websocket` 或 `webhook`

只有当你要接入自建 connector 时，才需要额外填写 `baseUrl`。

当前随插件一起分发的运行时产物路径为：

- `dist/feishu-connector.jar`

## 推荐接入方式

当前仍建议优先使用 `websocket` 模式，这样不需要把知微主服务直接暴露到公网。

如果你的部署环境已经具备稳定公网入口，可切换到 `webhook` 模式。此时需要同时填写：

- `verificationToken`
- `encryptKey`

## 运行时约定

connector 应实现以下接口：

- `POST /instances/{instanceId}/start`
- `POST /instances/{instanceId}/stop`
- `POST /instances/{instanceId}/reload`
- `GET /instances/{instanceId}/health`
- `POST /instances/{instanceId}/deliver`
- `POST /instances/{instanceId}/webhook`

同时向知微主服务回调：

- `POST /api/channel-runtime/instances/{instanceId}/events`
- `POST /api/channel-runtime/instances/{instanceId}/heartbeat`

## 当前实现

- 工程目录：`connectors/feishu-connector`
- 分发产物：`channels/feishu/dist/feishu-connector.jar`
- 已接通：飞书 WebSocket 事件订阅、飞书 webhook 事件回调、统一事件回调、统一出站投递
- 已支持：文本消息、富文本消息、卡片消息、卡片动作回调、线程回复、同一 `responseId` 下的消息更新、原生图片上传发送、原生文件上传发送
- 当前限制：暂不支持更细粒度的原生媒体类型映射，`streaming` 仍会降级成飞书卡片

## 卡片动作约定

如果卡片 `payload.actions[]` 中的动作项包含：

- `label`
- 可选 `name`
- 可选 `value` 对象

且没有 `url`，connector 会把它渲染为飞书回调按钮，并在点击后向知微主服务回调一个 `content.type=card-action` 的统一事件。

如果知微主服务对本次动作返回了文本投递，connector 会优先把第一条文本投递提取成飞书即时 toast。
