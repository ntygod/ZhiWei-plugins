package com.lifepilot.connectors.feishu.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lifepilot.connectors.feishu.config.ConnectorRuntimeProperties;
import com.lifepilot.connectors.feishu.model.ConnectorDeliveryRequest;
import com.lifepilot.connectors.feishu.model.ConnectorEventResponse;
import com.lifepilot.connectors.feishu.model.ConnectorInstanceCommandRequest;
import com.lifepilot.connectors.feishu.model.ConnectorOperationRequest;
import com.lifepilot.connectors.feishu.model.ConnectorOperationResponse;
import com.lifepilot.connectors.feishu.streaming.StreamingDeliveryQueue;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link FeishuConnectorService} 单元测试。
 *
 * @author zsg
 * @since 2026-03-29
 */
class FeishuConnectorServiceTest {

    @Test
    void start和deliver_应启动飞书实例并记录投递次数() throws Exception {
        ConnectorCallbackClient callbackClient = mock(ConnectorCallbackClient.class);
        FeishuPlatformSessionFactory sessionFactory = mock(FeishuPlatformSessionFactory.class);
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(sessionFactory.create(any(), any())).thenReturn(session);
        when(session.send(any())).thenReturn(new FeishuSendResult(FeishuSendRoute.CREATE, "text", "om_1"));

        FeishuConnectorService service = new FeishuConnectorService(callbackClient, sessionFactory, new ObjectMapper(), runtimeProperties(), new StreamingDeliveryQueue());

        service.start("feishu-1", "runtime-token", new ConnectorInstanceCommandRequest(
                "feishu-1",
                "feishu",
                "feishu",
                Map.of(
                        "baseUrl", "http://127.0.0.1:19091",
                        "appId", "cli_mock",
                        "connectionMode", "websocket"
                ),
                Map.of("appSecret", "secret"),
                null
        ));
        Map<String, Object> deliveryResult = service.deliver("feishu-1", new ConnectorDeliveryRequest(
                "feishu-1",
                "resp-1",
                "ASYNC_PUSH",
                new ConnectorDeliveryRequest.Target("ou_1", "oc_1", Map.of()),
                new ConnectorDeliveryRequest.Content("text", "hello", Map.of("text", "hello")),
                List.of(),
                Map.of()
        ));
        Map<String, Object> health = service.health("feishu-1");

        assertThat(deliveryResult).containsEntry("accepted", true);
        assertThat(deliveryResult).containsEntry("deliveryCount", 1);
        assertThat(health).containsEntry("healthy", true);
        assertThat(health).containsEntry("deliveryCount", 1);
        assertThat(health).containsEntry("platform", "feishu");
        verify(session).start();
        verify(session).send(any());
    }

    @Test
    void start_自动托管模式下不填BaseUrl也应成功启动() {
        ConnectorCallbackClient callbackClient = mock(ConnectorCallbackClient.class);
        FeishuPlatformSessionFactory sessionFactory = mock(FeishuPlatformSessionFactory.class);
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(sessionFactory.create(any(), any())).thenReturn(session);

        FeishuConnectorService service = new FeishuConnectorService(callbackClient, sessionFactory, new ObjectMapper(), runtimeProperties(), new StreamingDeliveryQueue());

        Map<String, Object> result = service.start("feishu-1", "runtime-token", new ConnectorInstanceCommandRequest(
                "feishu-1",
                "feishu",
                "feishu",
                Map.of(
                        "appId", "cli_mock",
                        "connectionMode", "websocket"
                ),
                Map.of("appSecret", "secret"),
                null
        ));

        assertThat(result).containsEntry("status", "RUNNING");
        verify(session).start();
    }

    @Test
    void handleInboundMessage_应回调知微并投递返回结果() throws Exception {
        ConnectorCallbackClient callbackClient = mock(ConnectorCallbackClient.class);
        FeishuPlatformSessionFactory sessionFactory = mock(FeishuPlatformSessionFactory.class);
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(sessionFactory.create(any(), any())).thenReturn(session);
        when(session.send(any())).thenReturn(new FeishuSendResult(FeishuSendRoute.REPLY, "text", "om_2"));
        when(callbackClient.submitEvent(eq("feishu-1"), eq("runtime-token"), any()))
                .thenReturn(new ConnectorEventResponse(
                        true,
                        "resp-1",
                        200,
                        null,
                        List.of(Map.of(
                                "instanceId", "feishu-1",
                                "responseId", "resp-1",
                                "deliveryMode", "ASYNC_PUSH",
                                "target", Map.of(
                                        "userId", "ou_1",
                                        "sessionId", "oc_1",
                                        "attributes", Map.of(
                                                "messageId", "om_1",
                                                "threadId", "th_1"
                                        )
                                ),
                                "content", Map.of(
                                        "type", "text",
                                        "plainText", "收到",
                                        "payload", Map.of("text", "收到")
                                ),
                                "attachments", List.of(),
                                "metadata", Map.of()
                        ))
                ));

        FeishuConnectorService service = new FeishuConnectorService(callbackClient, sessionFactory, new ObjectMapper(), runtimeProperties(), new StreamingDeliveryQueue());
        service.start("feishu-1", "runtime-token", new ConnectorInstanceCommandRequest(
                "feishu-1",
                "feishu",
                "feishu",
                Map.of(
                        "baseUrl", "http://127.0.0.1:19091",
                        "appId", "cli_mock",
                        "connectionMode", "websocket"
                ),
                Map.of("appSecret", "secret"),
                null
        ));

        service.handleInboundMessage("feishu-1", new FeishuInboundMessage(
                "evt-1",
                "om_1",
                "text",
                "oc_1",
                "p2p",
                "th_1",
                null,
                null,
                "ou_1",
                "u_1",
                null,
                "user",
                "tenant-1",
                "{\"text\":\"你好，知微\"}",
                Instant.parse("2026-03-29T12:00:00Z")
        ));
        Map<String, Object> health = service.health("feishu-1");

        assertThat(health).containsEntry("inboundEventCount", 1);
        assertThat(health).containsEntry("lastChatId", "oc_1");
        verify(callbackClient).submitEvent(eq("feishu-1"), eq("runtime-token"), any());
        verify(session).send(any());
    }

    @Test
    void handleInboundMessage_卡片动作应转成cardAction事件() {
        ConnectorCallbackClient callbackClient = mock(ConnectorCallbackClient.class);
        FeishuPlatformSessionFactory sessionFactory = mock(FeishuPlatformSessionFactory.class);
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(sessionFactory.create(any(), any())).thenReturn(session);
        when(callbackClient.submitEvent(eq("feishu-1"), eq("runtime-token"), any()))
                .thenReturn(new ConnectorEventResponse(true, "resp-card-1", 200, null, List.of()));

        FeishuConnectorService service = new FeishuConnectorService(callbackClient, sessionFactory, new ObjectMapper(), runtimeProperties(), new StreamingDeliveryQueue());
        service.start("feishu-1", "runtime-token", new ConnectorInstanceCommandRequest(
                "feishu-1",
                "feishu",
                "feishu",
                Map.of(
                        "baseUrl", "http://127.0.0.1:19091",
                        "appId", "cli_mock",
                        "connectionMode", "websocket"
                ),
                Map.of("appSecret", "secret"),
                null
        ));

        service.handleInboundMessage("feishu-1", new FeishuInboundMessage(
                "evt-card-1",
                "om_card_1",
                "card-action",
                "oc_card_1",
                null,
                null,
                null,
                null,
                "ou_card_1",
                "u_card_1",
                null,
                "user",
                "tenant-1",
                "{\"action\":{\"name\":\"approve\",\"label\":\"通过\",\"value\":{\"action\":\"approve\"}},\"context\":{\"openMessageId\":\"om_card_1\",\"openChatId\":\"oc_card_1\"}}",
                Instant.parse("2026-03-29T12:10:00Z")
        ));

        verify(callbackClient).submitEvent(eq("feishu-1"), eq("runtime-token"), argThat(request ->
                "card-action".equals(request.content().type())
                        && "触发了卡片动作: 通过".equals(request.content().text())
                        && "approve".equals(request.content().name())
                        && request.content().payload() != null
                        && request.content().payload().containsKey("action")
        ));
    }

    @Test
    void handleInboundMessage_卡片动作应返回知微结果toast() throws Exception {
        ConnectorCallbackClient callbackClient = mock(ConnectorCallbackClient.class);
        FeishuPlatformSessionFactory sessionFactory = mock(FeishuPlatformSessionFactory.class);
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(sessionFactory.create(any(), any())).thenReturn(session);
        when(session.send(any())).thenReturn(new FeishuSendResult(FeishuSendRoute.REPLY, "text", "om_reply_2"));
        when(callbackClient.submitEvent(eq("feishu-1"), eq("runtime-token"), any()))
                .thenReturn(new ConnectorEventResponse(
                        true,
                        "resp-card-2",
                        200,
                        null,
                        List.of(Map.of(
                                "instanceId", "feishu-1",
                                "responseId", "resp-card-2",
                                "deliveryMode", "ASYNC_PUSH",
                                "target", Map.of(
                                        "userId", "ou_card_2",
                                        "sessionId", "oc_card_2",
                                        "attributes", Map.of(
                                                "messageId", "om_card_2"
                                        )
                                ),
                                "content", Map.of(
                                        "type", "text",
                                        "plainText", "审批已通过，马上同步结果",
                                        "payload", Map.of("text", "审批已通过，马上同步结果")
                                ),
                                "attachments", List.of(),
                                "metadata", Map.of()
                        ))
                ));

        FeishuConnectorService service = new FeishuConnectorService(callbackClient, sessionFactory, new ObjectMapper(), runtimeProperties(), new StreamingDeliveryQueue());
        service.start("feishu-1", "runtime-token", new ConnectorInstanceCommandRequest(
                "feishu-1",
                "feishu",
                "feishu",
                Map.of(
                        "baseUrl", "http://127.0.0.1:19091",
                        "appId", "cli_mock",
                        "connectionMode", "websocket"
                ),
                Map.of("appSecret", "secret"),
                null
        ));

        FeishuInboundHandlingResult result = service.handleInboundMessage("feishu-1", new FeishuInboundMessage(
                "evt-card-2",
                "om_card_2",
                "card-action",
                "oc_card_2",
                null,
                null,
                null,
                null,
                "ou_card_2",
                "u_card_2",
                null,
                "user",
                "tenant-1",
                "{\"action\":{\"name\":\"approve\",\"label\":\"通过\"}}",
                Instant.parse("2026-03-29T12:15:00Z")
        ));

        assertThat(result.accepted()).isTrue();
        assertThat(result.toastContent()).isEqualTo("审批已通过，马上同步结果");
    }

    @Test
    void 同一responseId再次投递卡片时应走patch更新() throws Exception {
        ConnectorCallbackClient callbackClient = mock(ConnectorCallbackClient.class);
        FeishuPlatformSessionFactory sessionFactory = mock(FeishuPlatformSessionFactory.class);
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(sessionFactory.create(any(), any())).thenReturn(session);
        when(session.send(any()))
                .thenReturn(new FeishuSendResult(FeishuSendRoute.CREATE, "interactive", "om_1"))
                .thenReturn(new FeishuSendResult(FeishuSendRoute.PATCH, "interactive", "om_1"));

        FeishuConnectorService service = new FeishuConnectorService(callbackClient, sessionFactory, new ObjectMapper(), runtimeProperties(), new StreamingDeliveryQueue());
        service.start("feishu-1", "runtime-token", new ConnectorInstanceCommandRequest(
                "feishu-1",
                "feishu",
                "feishu",
                Map.of(
                        "baseUrl", "http://127.0.0.1:19091",
                        "appId", "cli_mock",
                        "connectionMode", "websocket"
                ),
                Map.of("appSecret", "secret"),
                null
        ));

        service.deliver("feishu-1", new ConnectorDeliveryRequest(
                "feishu-1",
                "resp-1",
                "ASYNC_PUSH",
                new ConnectorDeliveryRequest.Target("ou_1", "oc_1", Map.of()),
                new ConnectorDeliveryRequest.Content("card", "第一条卡片", Map.of(
                        "title", "初始标题",
                        "body", "第一条卡片"
                )),
                List.of(),
                Map.of()
        ));
        service.deliver("feishu-1", new ConnectorDeliveryRequest(
                "feishu-1",
                "resp-1",
                "ASYNC_PUSH",
                new ConnectorDeliveryRequest.Target("ou_1", "oc_1", Map.of()),
                new ConnectorDeliveryRequest.Content("card", "卡片正文", Map.of(
                        "title", "更新标题",
                        "body", "卡片正文"
                )),
                List.of(),
                Map.of()
        ));

        verify(session).send(argThat(message ->
                message.route() == FeishuSendRoute.PATCH
                        && "interactive".equals(message.msgType())
                        && "om_1".equals(message.targetMessageId())
        ));
    }

    @Test
    void card消息的回调按钮应生成飞书value载荷() throws Exception {
        ConnectorCallbackClient callbackClient = mock(ConnectorCallbackClient.class);
        FeishuPlatformSessionFactory sessionFactory = mock(FeishuPlatformSessionFactory.class);
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(sessionFactory.create(any(), any())).thenReturn(session);
        when(session.send(any())).thenReturn(new FeishuSendResult(FeishuSendRoute.CREATE, "interactive", "om_1"));

        FeishuConnectorService service = new FeishuConnectorService(callbackClient, sessionFactory, new ObjectMapper(), runtimeProperties(), new StreamingDeliveryQueue());
        service.start("feishu-1", "runtime-token", new ConnectorInstanceCommandRequest(
                "feishu-1",
                "feishu",
                "feishu",
                Map.of(
                        "baseUrl", "http://127.0.0.1:19091",
                        "appId", "cli_mock",
                        "connectionMode", "websocket"
                ),
                Map.of("appSecret", "secret"),
                null
        ));

        service.deliver("feishu-1", new ConnectorDeliveryRequest(
                "feishu-1",
                "resp-card-action-1",
                "ASYNC_PUSH",
                new ConnectorDeliveryRequest.Target("ou_1", "oc_1", Map.of()),
                new ConnectorDeliveryRequest.Content("card", "审批卡片", Map.of(
                        "title", "审批",
                        "body", "请选择操作",
                        "actions", List.of(
                                Map.of(
                                        "label", "通过",
                                        "type", "callback",
                                        "value", "approve"
                                )
                        )
                )),
                List.of(),
                Map.of()
        ));

        verify(session).send(argThat(message ->
                "interactive".equals(message.msgType())
                        && message.content().contains("\"type\":\"callback\"")
                        && message.content().contains("\"action\":\"approve\"")
        ));
    }

    @Test
    void start_在webhook模式下应启动实例并暴露webhook模式健康状态() {
        ConnectorCallbackClient callbackClient = mock(ConnectorCallbackClient.class);
        FeishuPlatformSessionFactory sessionFactory = mock(FeishuPlatformSessionFactory.class);
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(sessionFactory.create(any(), any())).thenReturn(session);
        FeishuConnectorService service = new FeishuConnectorService(callbackClient, sessionFactory, new ObjectMapper(), runtimeProperties(), new StreamingDeliveryQueue());

        service.start("feishu-1", "runtime-token", new ConnectorInstanceCommandRequest(
                "feishu-1",
                "feishu",
                "feishu",
                Map.of(
                        "baseUrl", "http://127.0.0.1:19091",
                        "appId", "cli_mock",
                        "connectionMode", "webhook"
                ),
                Map.of(
                        "appSecret", "secret",
                        "verificationToken", "token",
                        "encryptKey", "encrypt"
                ),
                null
        ));
        Map<String, Object> health = service.health("feishu-1");

        assertThat(health).containsEntry("connectionMode", "webhook");
        assertThat(health).containsEntry("healthy", true);
        assertThat(health.get("supportedModes")).isEqualTo(List.of("websocket", "webhook"));
        verify(session).start();
    }

    @Test
    void handleWebhook_应委托给实例会话() throws Exception {
        ConnectorCallbackClient callbackClient = mock(ConnectorCallbackClient.class);
        FeishuPlatformSessionFactory sessionFactory = mock(FeishuPlatformSessionFactory.class);
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(sessionFactory.create(any(), any())).thenReturn(session);
        when(session.handleWebhook(any())).thenReturn(new FeishuWebhookResponse(
                200,
                "{\"msg\":\"success\"}".getBytes(),
                Map.of("Content-Type", List.of("application/json"))
        ));

        FeishuConnectorService service = new FeishuConnectorService(callbackClient, sessionFactory, new ObjectMapper(), runtimeProperties(), new StreamingDeliveryQueue());
        service.start("feishu-1", "runtime-token", new ConnectorInstanceCommandRequest(
                "feishu-1",
                "feishu",
                "feishu",
                Map.of(
                        "baseUrl", "http://127.0.0.1:19091",
                        "appId", "cli_mock",
                        "connectionMode", "webhook"
                ),
                Map.of(
                        "appSecret", "secret",
                        "verificationToken", "token",
                        "encryptKey", "encrypt"
                ),
                null
        ));

        FeishuWebhookResponse response = service.handleWebhook("feishu-1", new FeishuWebhookRequest(
                "/instances/feishu-1/webhook",
                "{\"type\":\"url_verification\"}".getBytes(),
                Map.of()
        ));

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(new String(response.body())).contains("success");
        verify(session).handleWebhook(any());
    }

    @Test
    void image消息应优先上传并发送原生图片() throws Exception {
        ConnectorCallbackClient callbackClient = mock(ConnectorCallbackClient.class);
        FeishuPlatformSessionFactory sessionFactory = mock(FeishuPlatformSessionFactory.class);
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(sessionFactory.create(any(), any())).thenReturn(session);
        when(session.uploadImage(any())).thenReturn("img_v2_1");
        when(session.send(any())).thenReturn(new FeishuSendResult(FeishuSendRoute.CREATE, "image", "om_img_1"));

        FeishuConnectorService service = new FeishuConnectorService(callbackClient, sessionFactory, new ObjectMapper(), runtimeProperties(), new StreamingDeliveryQueue());
        service.start("feishu-1", "runtime-token", new ConnectorInstanceCommandRequest(
                "feishu-1",
                "feishu",
                "feishu",
                Map.of(
                        "baseUrl", "http://127.0.0.1:19091",
                        "appId", "cli_mock",
                        "connectionMode", "websocket"
                ),
                Map.of("appSecret", "secret"),
                null
        ));

        Map<String, Object> result = service.deliver("feishu-1", new ConnectorDeliveryRequest(
                "feishu-1",
                "resp-image-1",
                "ASYNC_PUSH",
                new ConnectorDeliveryRequest.Target("ou_1", "oc_1", Map.of()),
                new ConnectorDeliveryRequest.Content("image", "图片说明", Map.of("caption", "图片说明")),
                List.of(new ConnectorDeliveryRequest.Attachment(
                        "att-1",
                        "answer.png",
                        "image/png",
                        "aGVsbG8=",
                        5
                )),
                Map.of()
        ));

        assertThat(result).containsEntry("contentType", "image");
        verify(session).uploadImage(any());
        verify(session).send(argThat(message ->
                message.route() == FeishuSendRoute.CREATE
                        && "image".equals(message.msgType())
                        && message.content().contains("\"image_key\":\"img_v2_1\"")
        ));
    }

    @Test
    void image上传失败时应降级为卡片消息() throws Exception {
        ConnectorCallbackClient callbackClient = mock(ConnectorCallbackClient.class);
        FeishuPlatformSessionFactory sessionFactory = mock(FeishuPlatformSessionFactory.class);
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(sessionFactory.create(any(), any())).thenReturn(session);
        when(session.uploadImage(any())).thenThrow(new IllegalStateException("上传失败"));
        when(session.send(any())).thenReturn(new FeishuSendResult(FeishuSendRoute.CREATE, "interactive", "om_card_1"));

        FeishuConnectorService service = new FeishuConnectorService(callbackClient, sessionFactory, new ObjectMapper(), runtimeProperties(), new StreamingDeliveryQueue());
        service.start("feishu-1", "runtime-token", new ConnectorInstanceCommandRequest(
                "feishu-1",
                "feishu",
                "feishu",
                Map.of(
                        "baseUrl", "http://127.0.0.1:19091",
                        "appId", "cli_mock",
                        "connectionMode", "websocket"
                ),
                Map.of("appSecret", "secret"),
                null
        ));

        Map<String, Object> result = service.deliver("feishu-1", new ConnectorDeliveryRequest(
                "feishu-1",
                "resp-image-2",
                "ASYNC_PUSH",
                new ConnectorDeliveryRequest.Target("ou_1", "oc_1", Map.of()),
                new ConnectorDeliveryRequest.Content("image", "图片说明", Map.of("caption", "图片说明")),
                List.of(new ConnectorDeliveryRequest.Attachment(
                        "att-1",
                        "answer.png",
                        "image/png",
                        "aGVsbG8=",
                        5
                )),
                Map.of()
        ));

        assertThat(result).containsEntry("contentType", "interactive");
        verify(session).send(argThat(message ->
                message.route() == FeishuSendRoute.CREATE
                        && "interactive".equals(message.msgType())
        ));
    }

    @Test
    void file消息应优先上传并发送原生文件() throws Exception {
        ConnectorCallbackClient callbackClient = mock(ConnectorCallbackClient.class);
        FeishuPlatformSessionFactory sessionFactory = mock(FeishuPlatformSessionFactory.class);
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(sessionFactory.create(any(), any())).thenReturn(session);
        when(session.uploadFile(any())).thenReturn("file_v2_1");
        when(session.send(any())).thenReturn(new FeishuSendResult(FeishuSendRoute.CREATE, "file", "om_file_1"));

        FeishuConnectorService service = new FeishuConnectorService(callbackClient, sessionFactory, new ObjectMapper(), runtimeProperties(), new StreamingDeliveryQueue());
        service.start("feishu-1", "runtime-token", new ConnectorInstanceCommandRequest(
                "feishu-1",
                "feishu",
                "feishu",
                Map.of(
                        "baseUrl", "http://127.0.0.1:19091",
                        "appId", "cli_mock",
                        "connectionMode", "websocket"
                ),
                Map.of("appSecret", "secret"),
                null
        ));

        Map<String, Object> result = service.deliver("feishu-1", new ConnectorDeliveryRequest(
                "feishu-1",
                "resp-file-1",
                "ASYNC_PUSH",
                new ConnectorDeliveryRequest.Target("ou_1", "oc_1", Map.of()),
                new ConnectorDeliveryRequest.Content("file", "测试文件", Map.of("fileName", "report.pdf")),
                List.of(new ConnectorDeliveryRequest.Attachment(
                        "att-file-1",
                        "report.pdf",
                        "application/pdf",
                        "aGVsbG8=",
                        5
                )),
                Map.of()
        ));

        assertThat(result).containsEntry("contentType", "file");
        verify(session).uploadFile(any());
        verify(session).send(argThat(message ->
                message.route() == FeishuSendRoute.CREATE
                        && "file".equals(message.msgType())
                        && message.content().contains("\"file_key\":\"file_v2_1\"")
        ));
    }

    @Test
    void file上传失败时应降级为卡片消息() throws Exception {
        ConnectorCallbackClient callbackClient = mock(ConnectorCallbackClient.class);
        FeishuPlatformSessionFactory sessionFactory = mock(FeishuPlatformSessionFactory.class);
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(sessionFactory.create(any(), any())).thenReturn(session);
        when(session.uploadFile(any())).thenThrow(new IllegalStateException("上传失败"));
        when(session.send(any())).thenReturn(new FeishuSendResult(FeishuSendRoute.CREATE, "interactive", "om_file_card_1"));

        FeishuConnectorService service = new FeishuConnectorService(callbackClient, sessionFactory, new ObjectMapper(), runtimeProperties(), new StreamingDeliveryQueue());
        service.start("feishu-1", "runtime-token", new ConnectorInstanceCommandRequest(
                "feishu-1",
                "feishu",
                "feishu",
                Map.of(
                        "baseUrl", "http://127.0.0.1:19091",
                        "appId", "cli_mock",
                        "connectionMode", "websocket"
                ),
                Map.of("appSecret", "secret"),
                null
        ));

        Map<String, Object> result = service.deliver("feishu-1", new ConnectorDeliveryRequest(
                "feishu-1",
                "resp-file-2",
                "ASYNC_PUSH",
                new ConnectorDeliveryRequest.Target("ou_1", "oc_1", Map.of()),
                new ConnectorDeliveryRequest.Content("file", "测试文件", Map.of("fileName", "report.pdf")),
                List.of(new ConnectorDeliveryRequest.Attachment(
                        "att-file-1",
                        "report.pdf",
                        "application/pdf",
                        "aGVsbG8=",
                        5
                )),
                Map.of()
        ));

        assertThat(result).containsEntry("contentType", "interactive");
        verify(session).send(argThat(message ->
                message.route() == FeishuSendRoute.CREATE
                        && "interactive".equals(message.msgType())
        ));
    }

    // ========== handleOperation 操作端点测试 ==========

    @Test
    void handleOperation_message_update应正确更新消息() {
        FeishuConnectorService service = 启动标准实例();

        ConnectorOperationResponse response = service.handleOperation("feishu-1", new ConnectorOperationRequest(
                "feishu-1",
                "op-update-1",
                "message_update",
                Map.of(
                        "messageId", "om_1",
                        "msgType", "text",
                        "content", "{\"text\":\"更新后的内容\"}"
                ),
                null
        ));

        assertThat(response.success()).isTrue();
        assertThat(response.operationId()).isEqualTo("op-update-1");
        assertThat(response.result()).containsEntry("updated", true);
    }

    @Test
    void handleOperation_message_recall应正确撤回消息() {
        FeishuConnectorService service = 启动标准实例();

        ConnectorOperationResponse response = service.handleOperation("feishu-1", new ConnectorOperationRequest(
                "feishu-1",
                "op-recall-1",
                "message_recall",
                Map.of("messageId", "om_1"),
                null
        ));

        assertThat(response.success()).isTrue();
        assertThat(response.operationId()).isEqualTo("op-recall-1");
        assertThat(response.result()).containsEntry("recalled", true);
    }

    @Test
    void handleOperation_file_upload_image类型应委托uploadImage() throws Exception {
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(session.uploadImage(any())).thenReturn("img_v2_uploaded");
        FeishuConnectorService service = 启动标准实例(session);

        ConnectorOperationResponse response = service.handleOperation("feishu-1", new ConnectorOperationRequest(
                "feishu-1",
                "op-upload-1",
                "file_upload",
                Map.of(
                        "fileType", "image",
                        "base64Data", java.util.Base64.getEncoder().encodeToString("test-image".getBytes()),
                        "fileName", "test.png",
                        "mimeType", "image/png"
                ),
                null
        ));

        assertThat(response.success()).isTrue();
        assertThat(response.result()).containsEntry("imageKey", "img_v2_uploaded");
        assertThat(response.result()).containsEntry("fileType", "image");
        verify(session).uploadImage(any());
    }

    @Test
    void handleOperation_file_upload_非image类型应委托uploadFile() throws Exception {
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(session.uploadFile(any())).thenReturn("file_v2_uploaded");
        FeishuConnectorService service = 启动标准实例(session);

        ConnectorOperationResponse response = service.handleOperation("feishu-1", new ConnectorOperationRequest(
                "feishu-1",
                "op-upload-2",
                "file_upload",
                Map.of(
                        "fileType", "stream",
                        "base64Data", java.util.Base64.getEncoder().encodeToString("test-file".getBytes()),
                        "fileName", "report.pdf",
                        "mimeType", "application/pdf"
                ),
                null
        ));

        assertThat(response.success()).isTrue();
        assertThat(response.result()).containsEntry("fileKey", "file_v2_uploaded");
        assertThat(response.result()).containsEntry("fileType", "stream");
        verify(session).uploadFile(any());
    }

    @Test
    void handleOperation_file_download应委托downloadResource() throws Exception {
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(session.downloadResource("om_1", "file_key_1", "file"))
                .thenReturn(Map.of("base64Data", "dGVzdA==", "size", 4L));
        FeishuConnectorService service = 启动标准实例(session);

        ConnectorOperationResponse response = service.handleOperation("feishu-1", new ConnectorOperationRequest(
                "feishu-1",
                "op-download-1",
                "file_download",
                Map.of(
                        "messageId", "om_1",
                        "fileKey", "file_key_1",
                        "type", "file"
                ),
                null
        ));

        assertThat(response.success()).isTrue();
        assertThat(response.result()).containsEntry("base64Data", "dGVzdA==");
        verify(session).downloadResource("om_1", "file_key_1", "file");
    }

    @Test
    void handleOperation_group_create应委托createChat() throws Exception {
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(session.createChat(eq("测试群"), eq("群描述"), any()))
                .thenReturn(Map.of("chatId", "oc_new_1"));
        FeishuConnectorService service = 启动标准实例(session);

        ConnectorOperationResponse response = service.handleOperation("feishu-1", new ConnectorOperationRequest(
                "feishu-1",
                "op-group-1",
                "group_create",
                Map.of(
                        "name", "测试群",
                        "description", "群描述",
                        "userIdList", List.of("ou_1", "ou_2")
                ),
                null
        ));

        assertThat(response.success()).isTrue();
        assertThat(response.result()).containsEntry("chatId", "oc_new_1");
        verify(session).createChat(eq("测试群"), eq("群描述"), any());
    }

    @Test
    void handleOperation_group_member_manage_add应委托addChatMembers() throws Exception {
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(session.addChatMembers(eq("oc_1"), eq("open_id"), any()))
                .thenReturn(Map.of("added", 2));
        FeishuConnectorService service = 启动标准实例(session);

        ConnectorOperationResponse response = service.handleOperation("feishu-1", new ConnectorOperationRequest(
                "feishu-1",
                "op-member-1",
                "group_member_manage",
                Map.of(
                        "chatId", "oc_1",
                        "action", "add",
                        "memberIdType", "open_id",
                        "idList", List.of("ou_1", "ou_2")
                ),
                null
        ));

        assertThat(response.success()).isTrue();
        assertThat(response.result()).containsEntry("added", 2);
        verify(session).addChatMembers(eq("oc_1"), eq("open_id"), any());
    }

    @Test
    void handleOperation_group_member_manage_remove应委托removeChatMembers() throws Exception {
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(session.removeChatMembers(eq("oc_1"), eq("open_id"), any()))
                .thenReturn(Map.of("removed", 1));
        FeishuConnectorService service = 启动标准实例(session);

        ConnectorOperationResponse response = service.handleOperation("feishu-1", new ConnectorOperationRequest(
                "feishu-1",
                "op-member-2",
                "group_member_manage",
                Map.of(
                        "chatId", "oc_1",
                        "action", "remove",
                        "memberIdType", "open_id",
                        "idList", List.of("ou_3")
                ),
                null
        ));

        assertThat(response.success()).isTrue();
        assertThat(response.result()).containsEntry("removed", 1);
        verify(session).removeChatMembers(eq("oc_1"), eq("open_id"), any());
    }

    @Test
    void handleOperation_未知操作类型应返回失败() {
        FeishuConnectorService service = 启动标准实例();

        ConnectorOperationResponse response = service.handleOperation("feishu-1", new ConnectorOperationRequest(
                "feishu-1",
                "op-unknown-1",
                "unknown_operation",
                Map.of(),
                null
        ));

        assertThat(response.success()).isFalse();
        assertThat(response.operationId()).isEqualTo("op-unknown-1");
        assertThat(response.errorMessage()).contains("不支持的操作类型");
    }

    // ========== 高级 API 测试 ==========

    @Test
    void handleOperation_task_create应委托createTask() throws Exception {
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(session.createTask(eq("完成项目报告"), eq("1735689600")))
                .thenReturn(Map.of("taskId", "task_1", "summary", "完成项目报告"));
        FeishuConnectorService service = 启动标准实例(session);

        ConnectorOperationResponse response = service.handleOperation("feishu-1", new ConnectorOperationRequest(
                "feishu-1",
                "op-task-1",
                "task_create",
                Map.of(
                        "summary", "完成项目报告",
                        "dueTimestamp", "1735689600"
                ),
                null
        ));

        assertThat(response.success()).isTrue();
        assertThat(response.result()).containsEntry("taskId", "task_1");
        assertThat(response.result()).containsEntry("summary", "完成项目报告");
        verify(session).createTask("完成项目报告", "1735689600");
    }

    @Test
    void handleOperation_document_create应委托createDocument() throws Exception {
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(session.createDocument(eq("需求文档"), eq("folder_token_1")))
                .thenReturn(Map.of("documentId", "doc_1", "title", "需求文档"));
        FeishuConnectorService service = 启动标准实例(session);

        ConnectorOperationResponse response = service.handleOperation("feishu-1", new ConnectorOperationRequest(
                "feishu-1",
                "op-doc-1",
                "document_create",
                Map.of(
                        "title", "需求文档",
                        "folderToken", "folder_token_1"
                ),
                null
        ));

        assertThat(response.success()).isTrue();
        assertThat(response.result()).containsEntry("documentId", "doc_1");
        assertThat(response.result()).containsEntry("title", "需求文档");
        verify(session).createDocument("需求文档", "folder_token_1");
    }

    @Test
    void handleOperation_calendar_event_create应委托createCalendarEvent() throws Exception {
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(session.createCalendarEvent(eq("cal_1"), eq("团队周会"), eq("2026-04-02T10:00:00Z"), eq("2026-04-02T11:00:00Z")))
                .thenReturn(Map.of("eventId", "evt_1", "summary", "团队周会"));
        FeishuConnectorService service = 启动标准实例(session);

        ConnectorOperationResponse response = service.handleOperation("feishu-1", new ConnectorOperationRequest(
                "feishu-1",
                "op-cal-1",
                "calendar_event_create",
                Map.of(
                        "calendarId", "cal_1",
                        "summary", "团队周会",
                        "startTime", "2026-04-02T10:00:00Z",
                        "endTime", "2026-04-02T11:00:00Z"
                ),
                null
        ));

        assertThat(response.success()).isTrue();
        assertThat(response.result()).containsEntry("eventId", "evt_1");
        assertThat(response.result()).containsEntry("summary", "团队周会");
        verify(session).createCalendarEvent("cal_1", "团队周会", "2026-04-02T10:00:00Z", "2026-04-02T11:00:00Z");
    }

    @Test
    void handleOperation_操作执行异常应返回失败响应() throws Exception {
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(session.recallMessage("om_bad")).thenThrow(new RuntimeException("API 调用失败"));
        FeishuConnectorService service = 启动标准实例(session);

        ConnectorOperationResponse response = service.handleOperation("feishu-1", new ConnectorOperationRequest(
                "feishu-1",
                "op-error-1",
                "message_recall",
                Map.of("messageId", "om_bad"),
                null
        ));

        assertThat(response.success()).isFalse();
        assertThat(response.operationId()).isEqualTo("op-error-1");
        assertThat(response.errorMessage()).isEqualTo("API 调用失败");
    }

    @Test
    void handleOperation_缺少必要参数应返回失败响应() {
        FeishuConnectorService service = 启动标准实例();

        ConnectorOperationResponse response = service.handleOperation("feishu-1", new ConnectorOperationRequest(
                "feishu-1",
                "op-miss-1",
                "message_update",
                Map.of("msgType", "text"),
                null
        ));

        assertThat(response.success()).isFalse();
        assertThat(response.errorMessage()).contains("messageId");
    }

    // ========== 辅助方法 ==========

    /**
     * 启动一个标准实例，使用默认的 mock session（updateMessage/recallMessage 已配置返回值）。
     */
    private FeishuConnectorService 启动标准实例() {
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        try {
            when(session.updateMessage(any(), any(), any())).thenReturn(Map.of("updated", true));
            when(session.recallMessage(any())).thenReturn(Map.of("recalled", true));
        } catch (Exception ignored) {
        }
        return 启动标准实例(session);
    }

    /**
     * 启动一个标准实例，使用指定的 session mock。
     */
    private FeishuConnectorService 启动标准实例(FeishuPlatformSession session) {
        ConnectorCallbackClient callbackClient = mock(ConnectorCallbackClient.class);
        FeishuPlatformSessionFactory sessionFactory = mock(FeishuPlatformSessionFactory.class);
        when(sessionFactory.create(any(), any())).thenReturn(session);

        FeishuConnectorService service = new FeishuConnectorService(callbackClient, sessionFactory, new ObjectMapper(), runtimeProperties(), new StreamingDeliveryQueue());
        service.start("feishu-1", "runtime-token", new ConnectorInstanceCommandRequest(
                "feishu-1",
                "feishu",
                "feishu",
                Map.of(
                        "baseUrl", "http://127.0.0.1:19091",
                        "appId", "cli_mock",
                        "connectionMode", "websocket"
                ),
                Map.of("appSecret", "secret"),
                null
        ));
        return service;
    }

    private ConnectorRuntimeProperties runtimeProperties() {
        ConnectorRuntimeProperties properties = new ConnectorRuntimeProperties();
        properties.setImageMaxBytes(10 * 1024 * 1024);
        return properties;
    }
}
