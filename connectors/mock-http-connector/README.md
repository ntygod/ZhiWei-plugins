# Mock HTTP Connector

这是一个独立的渠道 connector 样板工程，用来验证知微当前的外部 connector 协议是否可用。

## 目标

- 提供一个最小可运行的 HTTP connector
- 响应知微控制面的 `start / stop / reload / health / deliver`
- 支持手动模拟一条入站消息，回调到知微主服务
- 作为后续飞书、企微、钉钉 connector 的最小开发基线

## 运行方式

```bash
mvn spring-boot:run -f plugins/connectors/mock-http-connector/pom.xml
```

默认端口：

- `19090`

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
- `POST /instances/{instanceId}/simulate-event`
- `POST /instances/{instanceId}/forward-heartbeat`

## 当前边界

这不是飞书、企微、钉钉的真实实现，只是协议样板。
