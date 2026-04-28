package com.lifepilot.connectors.feishu.streaming;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 流式投递队列单元测试 — 验证覆盖式 enqueue / responseId 隔离 / firstSeenAt 不变语义。
 *
 * @author zsg
 * @since 2026-04-28
 */
class StreamingDeliveryQueue_单元测试 {

    @Test
    void 同一_rid_高频_enqueue_只保留最新() {
        var queue = new StreamingDeliveryQueue();
        var instanceId = "i1";
        queue.enqueue(instanceId, "rid-1", "a", Instant.now());
        queue.enqueue(instanceId, "rid-1", "ab", Instant.now());
        queue.enqueue(instanceId, "rid-1", "abc", Instant.now());

        var entry = queue.peek("rid-1").orElseThrow();
        assertThat(entry.latestAccText()).isEqualTo("abc");
    }

    @Test
    void 不同_rid_互相隔离() {
        var queue = new StreamingDeliveryQueue();
        queue.enqueue("i1", "rid-A", "alpha", Instant.now());
        queue.enqueue("i1", "rid-B", "bravo", Instant.now());

        assertThat(queue.peek("rid-A").orElseThrow().latestAccText()).isEqualTo("alpha");
        assertThat(queue.peek("rid-B").orElseThrow().latestAccText()).isEqualTo("bravo");
    }

    @Test
    void enqueue_首次创建_firstSeenAt_后续保持不变() {
        var queue = new StreamingDeliveryQueue();
        Instant t0 = Instant.parse("2026-04-28T00:00:00Z");
        queue.enqueue("i1", "rid-1", "a", t0);
        queue.enqueue("i1", "rid-1", "ab", t0.plusMillis(500));

        var entry = queue.peek("rid-1").orElseThrow();
        assertThat(entry.firstSeenAt()).isEqualTo(t0);
        assertThat(entry.lastEnqueueAt()).isEqualTo(t0.plusMillis(500));
    }
}
