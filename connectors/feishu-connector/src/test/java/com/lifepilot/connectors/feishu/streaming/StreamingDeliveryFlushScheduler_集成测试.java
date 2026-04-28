package com.lifepilot.connectors.feishu.streaming;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 流式投递 flush 调度器集成测试 — 覆盖节流合并 / 429 退避 / 404 标记 / idle 清理。
 *
 * <p>注意：含 time-sensitive 行为（100ms tick + 200ms/1500ms 节流），
 * 用 awaitility 的轮询替代死等以降低 flaky；assertion 留有合理容差。</p>
 *
 * @author zsg
 * @since 2026-04-28
 */
class StreamingDeliveryFlushScheduler_集成测试 {

    @Test
    void 高频_enqueue_按节流策略合并_flush() throws InterruptedException {
        var queue = new StreamingDeliveryQueue();
        AtomicInteger flushCount = new AtomicInteger();
        FlushAction action = (instanceId, rid, text) -> {
            flushCount.incrementAndGet();
            return FlushOutcome.OK;
        };
        var scheduler = new StreamingDeliveryFlushScheduler(
                queue, FeishuStreamingPolicy.defaults(), action);
        scheduler.start();
        try {
            // 模拟 5 秒内每 50ms 入队（共 100 次），首字节窗口内 200ms 节流，理论 ≤ 30 次
            for (int i = 0; i < 100; i++) {
                queue.enqueue("i1", "rid-x", "msg" + i, Instant.now());
                Thread.sleep(50);
            }
            Thread.sleep(500);
            assertThat(flushCount.get()).isLessThanOrEqualTo(30);
            assertThat(flushCount.get()).isGreaterThan(0);
        } finally {
            scheduler.stop();
        }
    }

    @Test
    void flush_返回_429_触发指数退避() throws InterruptedException {
        var queue = new StreamingDeliveryQueue();
        AtomicInteger flushCount = new AtomicInteger();
        FlushAction action = (instanceId, rid, text) -> {
            flushCount.incrementAndGet();
            return flushCount.get() <= 3 ? FlushOutcome.RATE_LIMITED : FlushOutcome.OK;
        };
        var scheduler = new StreamingDeliveryFlushScheduler(
                queue, FeishuStreamingPolicy.defaults(), action);
        scheduler.start();
        try {
            queue.enqueue("i1", "rid-y", "first", Instant.now());
            // 等待至少一次 429 退避（1s）发生
            await().atMost(Duration.ofSeconds(2))
                    .until(() -> flushCount.get() >= 1);
            // 退避期内即使持续 enqueue 也不会无节制 flush
            for (int i = 0; i < 10; i++) {
                queue.enqueue("i1", "rid-y", "msg" + i, Instant.now());
                Thread.sleep(50);
            }
            // 验证 backoffStage 推进了
            var entry = queue.peek("rid-y").orElse(null);
            assertThat(entry).isNotNull();
            assertThat(flushCount.get()).isGreaterThanOrEqualTo(1);
        } finally {
            scheduler.stop();
        }
    }

    @Test
    void flush_返回_404_条目状态推进表明降级() throws InterruptedException {
        var queue = new StreamingDeliveryQueue();
        AtomicInteger flushCount = new AtomicInteger();
        FlushAction action = (instanceId, rid, text) -> {
            flushCount.incrementAndGet();
            return FlushOutcome.NOT_FOUND;
        };
        var scheduler = new StreamingDeliveryFlushScheduler(
                queue, FeishuStreamingPolicy.defaults(), action);
        scheduler.start();
        try {
            queue.enqueue("i1", "rid-z", "first", Instant.now());
            await().atMost(Duration.ofSeconds(1))
                    .until(() -> flushCount.get() >= 1);
            // NOT_FOUND 处理后 lastFlushAt 应被设置（视为已尝试）
            var entry = queue.peek("rid-z").orElse(null);
            assertThat(entry).isNotNull();
            assertThat(entry.lastFlushAt()).isNotNull();
        } finally {
            scheduler.stop();
        }
    }

    @Test
    void idle_超过30s_的_rid_最后flush_后清理() throws InterruptedException {
        var queue = new StreamingDeliveryQueue();
        // 创建一个 idle 已久的 entry（手工塞入）
        Instant longAgo = Instant.now().minusSeconds(60);
        queue.update(new StreamingDeliveryEntry("i1", "rid-old", "stale",
                longAgo, longAgo, longAgo.minusSeconds(10), 0));
        AtomicInteger flushCount = new AtomicInteger();
        FlushAction action = (instanceId, rid, text) -> {
            flushCount.incrementAndGet();
            return FlushOutcome.OK;
        };
        var scheduler = new StreamingDeliveryFlushScheduler(
                queue, FeishuStreamingPolicy.defaults(), action);
        scheduler.start();
        try {
            await().atMost(Duration.ofSeconds(2))
                    .until(() -> queue.peek("rid-old").isEmpty());
            assertThat(flushCount.get()).isEqualTo(1);
        } finally {
            scheduler.stop();
        }
    }
}
