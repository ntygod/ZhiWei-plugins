# Feishu Connector

这是知微官方飞书 connector 的首个可运行实现。

## 当前能力

- 响应知微控制面的 `start / stop / reload / health / deliver`
- 使用飞书官方 Java SDK 建立 `websocket` 长连接
- 支持飞书 `webhook` 事件订阅回调
- 订阅 `im.message.receive_v1` 事件并回调知微主服务
- 接收知微返回的统一投递结果，并自动回发到飞书
- 支持文本型回复和基于消息 ID 的线程回复
- 支持 `markdown -> post`、`card -> interactive`
- 支持交互卡片按钮回调，统一上送 `card-action` 事件
- 支持原生图片上传与 `image` 消息发送
- 支持原生文件上传与 `file` 消息发送
- 支持同一 `responseId` 的飞书消息更新：
  - 文本/富文本走 `update`
  - 卡片走 `patch`

## 当前边界

- 当前版本支持 `websocket` 和 `webhook`
- 暂不处理更细粒度的原生媒体类型映射
- `streaming` 当前仍会转成飞书卡片的降级展示

## 运行方式

```bash
mvn spring-boot:run -f connectors/feishu-connector/pom.xml
```

默认端口：

- `19091`

默认知微主服务地址：

- `http://127.0.0.1:8080`

可通过环境变量覆盖：

- `CONNECTOR_RUNTIME_ZHIWEI_BASE_URL`

## 已实现接口

- `POST /instances/{instanceId}/start`
- `POST /instances/{instanceId}/stop`
- `POST /instances/{instanceId}/reload`
- `GET /instances/{instanceId}/health`
- `POST /instances/{instanceId}/deliver`
- `POST /instances/{instanceId}/webhook`
- `POST /instances/{instanceId}/forward-heartbeat`

## 配置要求

必须提供：

- `baseUrl`
- `appId`
- `appSecret`
- `connectionMode`

可选：

- `verificationToken`
- `encryptKey`

当 `connectionMode=webhook` 时，`verificationToken` 和 `encryptKey` 为必填项。

## 卡片动作

卡片 `payload.actions[]` 中：

- 有 `url` 的动作会渲染成跳转按钮
- 没有 `url` 且有 `label` 的动作会渲染成回调按钮

回调按钮点击后，connector 会向知微主服务提交统一入站事件：

- `content.type = card-action`
- `content.payload.action`
- `content.payload.context`
- `content.payload.operator`

如果知微主服务对这次动作返回了文本投递，connector 会优先把第一条文本投递提取成飞书即时 toast。
