package com.lifepilot.connectors.qq.qq;

import com.lifepilot.connectors.qq.model.ConnectorEventRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * QqEventHandler 事件转换测试。
 *
 * @author zsg
 * @since 2026-04-03
 */
class QqEventHandler_事件转换测试 {

    private QqEventHandler handler;

    @BeforeEach
    void setUp() {
        handler = new QqEventHandler("qq.test");
    }

    @Test
    void 群at消息转换为统一事件() {
        var data = Map.<String, Object>of(
                "id", "msg-001",
                "group_openid", "group-abc",
                "author", Map.of("member_openid", "user-123"),
                "content", " 你好世界 ",
                "timestamp", "2026-04-03T10:00:00Z"
        );
        var event = new QqWebSocketManager.QqDispatchEvent("GROUP_AT_MESSAGE_CREATE", data);

        ConnectorEventRequest result = handler.convert(event);

        assertNotNull(result);
        assertEquals("user-123", result.userId());
        assertEquals("qq.test:group:group-abc", result.sessionId());
        assertEquals("text", result.content().type());
        assertEquals("你好世界", result.content().text());
        assertEquals("msg-001", result.messageId());

        // 验证 replyTarget
        assertNotNull(result.replyTarget());
        assertEquals("group", result.replyTarget().attributes().get("msgType"));
        assertEquals("group-abc", result.replyTarget().attributes().get("groupOpenId"));
        assertEquals("msg-001", result.replyTarget().attributes().get("msgId"));
    }

    @Test
    void 私聊消息转换为统一事件() {
        var data = Map.<String, Object>of(
                "id", "msg-002",
                "author", Map.of("user_openid", "user-456"),
                "content", "你好",
                "timestamp", "2026-04-03T10:00:00Z"
        );
        var event = new QqWebSocketManager.QqDispatchEvent("C2C_MESSAGE_CREATE", data);

        ConnectorEventRequest result = handler.convert(event);

        assertNotNull(result);
        assertEquals("user-456", result.userId());
        assertEquals("qq.test:c2c:user-456", result.sessionId());
        assertEquals("text", result.content().type());
        assertEquals("你好", result.content().text());

        // 验证 replyTarget
        assertNotNull(result.replyTarget());
        assertEquals("c2c", result.replyTarget().attributes().get("msgType"));
        assertEquals("user-456", result.replyTarget().attributes().get("userOpenId"));
    }

    @Test
    void 不支持的事件类型返回null() {
        var event = new QqWebSocketManager.QqDispatchEvent("UNKNOWN_EVENT", Map.of());

        ConnectorEventRequest result = handler.convert(event);

        assertNull(result);
    }

    @Test
    void 重复事件ID被去重() {
        var data = Map.<String, Object>of(
                "id", "msg-dup",
                "group_openid", "group-abc",
                "author", Map.of("member_openid", "user-123"),
                "content", "重复测试",
                "timestamp", "2026-04-03T10:00:00Z"
        );
        var event = new QqWebSocketManager.QqDispatchEvent("GROUP_AT_MESSAGE_CREATE", data);

        ConnectorEventRequest first = handler.convert(event);
        ConnectorEventRequest second = handler.convert(event);

        assertNotNull(first, "第一次应正常返回");
        assertNull(second, "第二次相同事件应被去重");
    }

    @Test
    void 群消息设置变更转换为lifecycle事件() {
        var data = Map.<String, Object>of("group_openid", "group-xyz");
        var event = new QqWebSocketManager.QqDispatchEvent("GROUP_MSG_REJECT", data);

        ConnectorEventRequest result = handler.convert(event);

        assertNotNull(result);
        assertEquals("event", result.content().type());
        assertEquals("group_msg_reject", result.content().name());
        assertEquals("system", result.userId());
    }

    @Test
    void 含附件的消息提取attachment() {
        var attachment = Map.<String, Object>of(
                "content_type", "image/png",
                "url", "https://example.com/img.png",
                "filename", "img.png"
        );
        var data = Map.<String, Object>of(
                "id", "msg-att",
                "group_openid", "group-abc",
                "author", Map.of("member_openid", "user-123"),
                "content", "看图",
                "timestamp", "2026-04-03T10:00:00Z",
                "attachments", List.of(attachment)
        );
        var event = new QqWebSocketManager.QqDispatchEvent("GROUP_AT_MESSAGE_CREATE", data);

        ConnectorEventRequest result = handler.convert(event);

        assertNotNull(result);
        assertEquals("image", result.content().type());
        assertEquals(1, result.attachments().size());
        assertEquals("image/png", result.attachments().getFirst().mimeType());
        assertEquals("img.png", result.attachments().getFirst().fileName());
    }

    @Test
    void 时间戳解析失败使用当前时间() {
        var data = Map.<String, Object>of(
                "id", "msg-ts",
                "author", Map.of("user_openid", "user-789"),
                "content", "测试",
                "timestamp", "invalid-timestamp"
        );
        var event = new QqWebSocketManager.QqDispatchEvent("C2C_MESSAGE_CREATE", data);

        ConnectorEventRequest result = handler.convert(event);

        assertNotNull(result);
        assertNotNull(result.occurredAt(), "解析失败应回退到当前时间");
    }
}
