package com.lifepilot.connectors.qq.qq;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * QqMessageSender 消息分段逻辑测试。
 *
 * @author zsg
 * @since 2026-04-03
 */
class QqMessageSender_消息分段测试 {

    @Test
    void 短文本不分段() {
        List<String> segments = QqMessageSender.segmentText("你好世界", 100);

        assertEquals(1, segments.size());
        assertEquals("你好世界", segments.getFirst());
    }

    @Test
    void 空文本返回单个空字符串() {
        List<String> segments = QqMessageSender.segmentText("", 100);

        assertEquals(1, segments.size());
        assertEquals("", segments.getFirst());
    }

    @Test
    void null文本返回单个空字符串() {
        List<String> segments = QqMessageSender.segmentText(null, 100);

        assertEquals(1, segments.size());
        assertEquals("", segments.getFirst());
    }

    @Test
    void 超长文本在换行处断开() {
        String text = "第一行内容\n第二行内容\n第三行内容";
        List<String> segments = QqMessageSender.segmentText(text, 10);

        assertTrue(segments.size() > 1, "应被分为多段");
        // 每段都不超过限制
        for (String segment : segments) {
            assertTrue(segment.length() <= 10, "段长度不应超过限制: " + segment);
        }
        // 拼接后内容完整
        assertEquals(text, String.join("", segments));
    }

    @Test
    void 恰好等于最大长度不分段() {
        String text = "a".repeat(100);
        List<String> segments = QqMessageSender.segmentText(text, 100);

        assertEquals(1, segments.size());
    }

    @Test
    void 超过最大长度且无断点强制截断() {
        String text = "a".repeat(200);
        List<String> segments = QqMessageSender.segmentText(text, 100);

        assertEquals(2, segments.size());
        assertEquals(100, segments.get(0).length());
        assertEquals(100, segments.get(1).length());
    }
}
