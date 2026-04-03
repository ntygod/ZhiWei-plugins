# QQ 机器人渠道插件

将知微 AI Agent 接入 QQ 群聊和私聊。

## 前置条件

1. 在 [QQ 开放平台](https://q.qq.com) 注册并创建机器人应用
2. 获取 **App ID** 和 **App Secret**
3. 在机器人管理后台开启以下权限：
   - 群聊消息（GROUP_AND_C2C_EVENT intent）
   - 私聊消息

## 配置项

| 字段 | 必填 | 说明 |
|------|------|------|
| App ID | 是 | QQ 机器人 AppID |
| App Secret | 是 | QQ 机器人 AppSecret |
| Connector Base URL | 否 | 自建 connector 地址，留空则自动托管 |

## 使用方式

- **群聊**: 将机器人添加到 QQ 群，在群中 @机器人 发送消息
- **私聊**: 直接向机器人发送消息

## 支持的消息类型

- 文本消息（发送/接收）
- 图片消息（接收，发送需后续版本支持）

## 技术说明

连接器通过 WebSocket 接入 QQ 机器人网关，自动处理鉴权、心跳和断线重连。
