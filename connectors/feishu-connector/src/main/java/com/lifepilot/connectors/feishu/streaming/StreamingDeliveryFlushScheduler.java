package com.lifepilot.connectors.feishu.streaming;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 流式投递 flush 调度器 — 100ms tick，按 policy 决定何时调 flushAction。
 *
 * <p>责任：
 * <ul>
 *   <li>每 100ms 扫描 queue 快照，对每个 rid 检查是否到达 minInterval</li>
 *   <li>到达则调 flushAction（飞书 message.update 或 send）</li>
 *   <li>根据 outcome 更新条目状态：成功→更新 lastFlushAt；429→加 backoff；
 *       404→视为已尝试；5xx→silent log</li>
 *   <li>idle 超过 30s 的 rid 强制 flush 一次后清理映射</li>
 * </ul>
 *
 * <p>飞书 QPS 是 app 级共享资源，故 globalBackoffUntil 跨 rid 共用：
 * 任一 rid 触发 429 后，所有 rid 都暂停 flush，避免雪崩。</p>
 *
 * @author zsg
 * @since 2026-04-28
 */
public class StreamingDeliveryFlushScheduler {

    private static final Logger log = LoggerFactory.getLogger(StreamingDeliveryFlushScheduler.class);

    private final StreamingDeliveryQueue queue;
    private final FeishuStreamingPolicy policy;
    private final FlushAction action;
    private final ScheduledExecutorService scheduler;
    private final AtomicReference<Instant> globalBackoffUntil = new AtomicReference<>(Instant.EPOCH);

    public StreamingDeliveryFlushScheduler(StreamingDeliveryQueue queue,
                                           FeishuStreamingPolicy policy,
                                           FlushAction action) {
        this.queue = queue;
        this.policy = policy;
        this.action = action;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "feishu-streaming-flusher");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 启动周期性 tick（100ms）。
     */
    public void start() {
        scheduler.scheduleAtFixedRate(this::tick, 100, 100, TimeUnit.MILLISECONDS);
    }

    /**
     * 关闭调度器；线程为 daemon，不阻塞 JVM 退出。
     */
    public void stop() {
        scheduler.shutdownNow();
    }

    private void tick() {
        try {
            Instant now = Instant.now();
            if (now.isBefore(globalBackoffUntil.get())) {
                return;  // 全局退避中，跳过本轮
            }
            for (StreamingDeliveryEntry entry : queue.snapshot()) {
                processEntry(entry, now);
            }
        } catch (Throwable t) {
            log.warn("flusher tick 异常: {}", t.getMessage(), t);
        }
    }

    private void processEntry(StreamingDeliveryEntry entry, Instant now) {
        boolean isIdle = policy.isIdle(entry.lastEnqueueAt(), now);
        Instant lastFlush = entry.lastFlushAt();
        Duration sinceLastFlush = lastFlush == null
                ? Duration.between(entry.firstSeenAt(), now)
                : Duration.between(lastFlush, now);
        Duration minInterval = policy.minInterval(entry.firstSeenAt(), now);

        // idle 强制 flush 一次后清理；非 idle 检查 minInterval
        if (!isIdle && sinceLastFlush.compareTo(minInterval) < 0) {
            return;
        }

        FlushOutcome outcome = action.flush(entry.instanceId(), entry.responseId(), entry.latestAccText());
        switch (outcome) {
            case OK -> {
                queue.update(entry.withFlushedAt(now));
                if (isIdle) {
                    queue.remove(entry.responseId());
                }
            }
            case RATE_LIMITED -> {
                int next = entry.backoffStage() + 1;
                queue.update(entry.withBackoff(next));
                Duration backoff = policy.backoffDuration(next - 1);
                globalBackoffUntil.set(now.plus(backoff));
                log.warn("飞书 429 退避: rid={}, stage={}, until={}",
                        entry.responseId(), next, now.plus(backoff));
            }
            case NOT_FOUND -> {
                // 主服务无感继续；标记已尝试，让下次循环过 minInterval 后再发（视作 send 新消息）
                queue.update(entry.withFlushedAt(now));
                log.info("飞书消息 404 — 视为撤回，标记已尝试: rid={}", entry.responseId());
            }
            case TRANSIENT -> log.debug("flush 5xx 静默重试: rid={}", entry.responseId());
        }
    }
}
