package com.lifepilot.connectors.feishu.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lifepilot.connectors.feishu.config.ConnectorRuntimeProperties;
import com.lifepilot.connectors.feishu.model.ConnectorDeliveryRequest;
import com.lifepilot.connectors.feishu.model.ConnectorInstanceCommandRequest;
import com.lifepilot.connectors.feishu.streaming.FlushOutcome;
import com.lifepilot.connectors.feishu.streaming.StreamingDeliveryEntry;
import com.lifepilot.connectors.feishu.streaming.StreamingDeliveryQueue;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link FeishuConnectorService} 流式投递相关行为测试。
 *
 * <p>覆盖：
 * <ul>
 *   <li>enqueue 路径：未启动实例 fail-fast / 启动后入队成功</li>
 *   <li>flush 路径：首次 send 保存 ref / 二次 update / 429 / 404 四种 outcome</li>
 * </ul>
 *
 * @author zsg
 * @since 2026-04-28
 */
class FeishuConnectorService_streaming_测试 {

    @Test
    void enqueueStreamingDelivery_未启动实例_应抛出_IllegalArgumentException() {
        StreamingDeliveryQueue queue = new StreamingDeliveryQueue();
        FeishuConnectorService service = newService(queue, mock(FeishuPlatformSession.class));

        ConnectorDeliveryRequest req = streamingDelivery("rid-1", "hello");

        assertThatThrownBy(() -> service.enqueueStreamingDelivery("feishu-1", req))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("实例尚未启动");
        assertThat(queue.peek("rid-1")).isEmpty();
    }

    @Test
    void enqueueStreamingDelivery_缺少_responseId_应抛出_IllegalArgumentException() throws Exception {
        StreamingDeliveryQueue queue = new StreamingDeliveryQueue();
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        FeishuConnectorService service = startedService(queue, session);

        ConnectorDeliveryRequest req = streamingDelivery(null, "hello");

        assertThatThrownBy(() -> service.enqueueStreamingDelivery("feishu-1", req))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("缺少 responseId");
    }

    @Test
    void enqueueStreamingDelivery_target缺sessionId且无lastChatId_应fail_fast() throws Exception {
        // 实例已启动但从未收过 inbound(lastChatId=null),target.sessionId 也为 null
        // 期望:抛 IllegalArgumentException 拒绝入队,避免下游 scheduler 死循环重试
        StreamingDeliveryQueue queue = new StreamingDeliveryQueue();
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        FeishuConnectorService service = startedService(queue, session);

        ConnectorDeliveryRequest req = new ConnectorDeliveryRequest(
                "feishu-1",
                "rid-no-chat",
                "PATCH_STREAM",
                new ConnectorDeliveryRequest.Target(null, null, Map.of()),
                new ConnectorDeliveryRequest.Content("text", "hello", Map.of("text", "hello")),
                List.of(),
                Map.of()
        );

        assertThatThrownBy(() -> service.enqueueStreamingDelivery("feishu-1", req))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("无可用 chatId");
        // 入队前抛错,队列不应留下任何条目
        assertThat(queue.peek("rid-no-chat")).isEmpty();
    }

    @Test
    void enqueueStreamingDelivery_启动后_应把最新_accText_快照入队() throws Exception {
        StreamingDeliveryQueue queue = new StreamingDeliveryQueue();
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        FeishuConnectorService service = startedService(queue, session);

        service.enqueueStreamingDelivery("feishu-1", streamingDelivery("rid-1", "msg-a"));
        service.enqueueStreamingDelivery("feishu-1", streamingDelivery("rid-1", "msg-a-b"));
        service.enqueueStreamingDelivery("feishu-1", streamingDelivery("rid-2", "x"));

        Optional<StreamingDeliveryEntry> rid1 = queue.peek("rid-1");
        Optional<StreamingDeliveryEntry> rid2 = queue.peek("rid-2");
        assertThat(rid1).isPresent();
        // 同一 rid 多次入队只保留最新一份快照
        assertThat(rid1.get().latestAccText()).isEqualTo("msg-a-b");
        assertThat(rid2).isPresent();
        assertThat(rid2.get().latestAccText()).isEqualTo("x");
        // SDK 不应被 enqueue 路径直接调用
        verify(session, never()).send(any());
        verify(session, never()).updateMessage(any(), any(), any());
    }

    @Test
    void flushStreamingDelivery_首次_应调_send_并保存_SentMessageRef() throws Exception {
        StreamingDeliveryQueue queue = new StreamingDeliveryQueue();
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(session.send(any()))
                .thenReturn(new FeishuSendResult(FeishuSendRoute.CREATE, "text", "om_1"));
        FeishuConnectorService service = startedServiceWithChat(queue, session);

        FlushOutcome outcome = service.flushStreamingDelivery("feishu-1", "rid-1", "hello");

        assertThat(outcome).isEqualTo(FlushOutcome.OK);
        verify(session).send(any());
        // 第二次同 rid flush 应走 update 路径，证明首次 send 已保存了 ref
        when(session.updateMessage(eq("om_1"), eq("text"), any()))
                .thenReturn(Map.of("messageId", "om_1"));
        FlushOutcome second = service.flushStreamingDelivery("feishu-1", "rid-1", "hello world");
        assertThat(second).isEqualTo(FlushOutcome.OK);
        verify(session).updateMessage(eq("om_1"), eq("text"), any());
    }

    @Test
    void flushStreamingDelivery_429异常_应映射为_RATE_LIMITED() throws Exception {
        StreamingDeliveryQueue queue = new StreamingDeliveryQueue();
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        // 飞书 SDK 触发频率限制时通常抛 IllegalStateException("飞书消息发送失败: code=99991663, ...")
        when(session.send(any()))
                .thenThrow(new IllegalStateException("飞书消息发送失败: code=429, message=rate limit"));
        FeishuConnectorService service = startedServiceWithChat(queue, session);

        FlushOutcome outcome = service.flushStreamingDelivery("feishu-1", "rid-rate", "msg");

        assertThat(outcome).isEqualTo(FlushOutcome.RATE_LIMITED);
    }

    @Test
    void flushStreamingDelivery_404异常_应映射为_NOT_FOUND_并删除_ref() throws Exception {
        StreamingDeliveryQueue queue = new StreamingDeliveryQueue();
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        // 先模拟首次 send 成功保存 ref
        when(session.send(any()))
                .thenReturn(new FeishuSendResult(FeishuSendRoute.CREATE, "text", "om_404"))
                .thenReturn(new FeishuSendResult(FeishuSendRoute.CREATE, "text", "om_404_new"));
        // 后续 update 抛 404 类异常
        when(session.updateMessage(eq("om_404"), eq("text"), any()))
                .thenThrow(new IllegalStateException("飞书消息发送失败: code=404, message=message not found"));
        FeishuConnectorService service = startedServiceWithChat(queue, session);

        // 首次 send → 保存 ref
        assertThat(service.flushStreamingDelivery("feishu-1", "rid-404", "first"))
                .isEqualTo(FlushOutcome.OK);
        // 二次 update → 命中 404 → 删 ref + NOT_FOUND
        assertThat(service.flushStreamingDelivery("feishu-1", "rid-404", "second"))
                .isEqualTo(FlushOutcome.NOT_FOUND);

        // 第三次同 rid 应再次走 send 路径（ref 已删）
        assertThat(service.flushStreamingDelivery("feishu-1", "rid-404", "third"))
                .isEqualTo(FlushOutcome.OK);
        // 验证 send 共被调 2 次（首次 + 删除 ref 后再次）
        verify(session, org.mockito.Mockito.times(2)).send(any());
    }

    @Test
    void flushStreamingDelivery_5xx异常_应映射为_TRANSIENT() throws Exception {
        StreamingDeliveryQueue queue = new StreamingDeliveryQueue();
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(session.send(any()))
                .thenThrow(new IllegalStateException("飞书消息发送失败: code=503, message=service unavailable"));
        FeishuConnectorService service = startedServiceWithChat(queue, session);

        FlushOutcome outcome = service.flushStreamingDelivery("feishu-1", "rid-5xx", "msg");

        assertThat(outcome).isEqualTo(FlushOutcome.TRANSIENT);
    }

    @Test
    void flushStreamingDelivery_未识别异常_应保守归类为_TRANSIENT() throws Exception {
        StreamingDeliveryQueue queue = new StreamingDeliveryQueue();
        FeishuPlatformSession session = mock(FeishuPlatformSession.class);
        when(session.send(any()))
                .thenThrow(new IllegalStateException("某种没人见过的飞书错误"));
        FeishuConnectorService service = startedServiceWithChat(queue, session);

        FlushOutcome outcome = service.flushStreamingDelivery("feishu-1", "rid-unknown", "msg");

        assertThat(outcome).isEqualTo(FlushOutcome.TRANSIENT);
    }

    /**
     * 构造一个未启动 instance 的 service。
     */
    private FeishuConnectorService newService(StreamingDeliveryQueue queue, FeishuPlatformSession session) {
        ConnectorCallbackClient callbackClient = mock(ConnectorCallbackClient.class);
        FeishuPlatformSessionFactory sessionFactory = mock(FeishuPlatformSessionFactory.class);
        when(sessionFactory.create(any(), any())).thenReturn(session);
        return new FeishuConnectorService(callbackClient, sessionFactory, new ObjectMapper(), runtimeProperties(), queue);
    }

    /**
     * 构造已启动但未收到任何入站消息的 service — flush 因缺 chatId 会失败，仅适合 enqueue 测试。
     */
    private FeishuConnectorService startedService(StreamingDeliveryQueue queue, FeishuPlatformSession session) {
        FeishuConnectorService service = newService(queue, session);
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

    /**
     * 构造已启动 + 已收到入站消息（lastChatId 已有值）的 service，让 flush 的 send 路径可走通。
     *
     * <p>flush 首次发送会读 state.lastChatId，未收到入站消息时会抛 IllegalStateException
     * 并被归 TRANSIENT。这里通过模拟一次 inbound 让 lastChatId 落库。</p>
     */
    private FeishuConnectorService startedServiceWithChat(StreamingDeliveryQueue queue, FeishuPlatformSession session) {
        FeishuConnectorService service = startedService(queue, session);
        // 直接 reflect 构造 inbound 不太合算；handleInboundMessage 涉及 callback，
        // 这里用 ad-hoc 方式：构造 minimal FeishuInboundMessage 喂给 handleInboundMessage 即可。
        // 但 handleInboundMessage 会 call callbackClient.submitEvent — mock 默认返回 null，
        // 因此真正的捷径是直接调 service 的 deliver 一次（deliver 也走 state.send 但不需要 chatId 来自 inbound，
        // 它从 request.target.sessionId 拿）。这样 deliver 后 state.lastDeliveredAt 等会更新但 lastChatId 仍为 null。
        //
        // 因 flush 强依赖 lastChatId，这里只能通过 handleInboundMessage 路径。简化方案：
        // 让 handleInboundMessage 收到一条 user 消息，callbackClient.submitEvent mock 返回拒绝（accepted=false），
        // 这样不会再触发后续 deliver，但 recordInboundEvent 会被调到，lastChatId 落库。
        ConnectorCallbackClient callbackClient = (ConnectorCallbackClient) extractCallbackClient(service);
        when(callbackClient.submitEvent(any(), any(), any()))
                .thenReturn(new com.lifepilot.connectors.feishu.model.ConnectorEventResponse(
                        false, null, 200, "rejected for test", List.of()));
        FeishuInboundMessage inbound = new FeishuInboundMessage(
                "ev_1",                      // eventId
                "msg_1",                     // messageId
                "text",                      // messageType
                "oc_1",                      // chatId
                "p2p",                       // chatType
                null,                        // threadId
                null,                        // rootId
                null,                        // parentId
                "ou_1",                      // senderOpenId
                null,                        // senderUserId
                null,                        // senderUnionId
                "user",                      // senderType
                null,                        // tenantKey
                "{\"text\":\"hi\"}",         // content
                java.time.Instant.now()      // occurredAt
        );
        service.handleInboundMessage("feishu-1", inbound);
        return service;
    }

    /**
     * 用反射从 service 拿 callbackClient — 仅为测试 setup 用，不污染生产代码 API。
     */
    private Object extractCallbackClient(FeishuConnectorService service) {
        try {
            java.lang.reflect.Field field = FeishuConnectorService.class.getDeclaredField("connectorCallbackClient");
            field.setAccessible(true);
            return field.get(service);
        } catch (ReflectiveOperationException ex) {
            throw new IllegalStateException("反射读取 callbackClient 失败", ex);
        }
    }

    private ConnectorDeliveryRequest streamingDelivery(String responseId, String text) {
        return new ConnectorDeliveryRequest(
                "feishu-1",
                responseId,
                "PATCH_STREAM",
                new ConnectorDeliveryRequest.Target("ou_1", "oc_1", Map.of()),
                new ConnectorDeliveryRequest.Content("text", text, Map.of()),
                List.of(),
                Map.of()
        );
    }

    private ConnectorRuntimeProperties runtimeProperties() {
        ConnectorRuntimeProperties properties = new ConnectorRuntimeProperties();
        properties.setImageMaxBytes(10 * 1024 * 1024);
        return properties;
    }
}
