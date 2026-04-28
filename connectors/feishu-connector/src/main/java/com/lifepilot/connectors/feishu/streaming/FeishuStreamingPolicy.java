package com.lifepilot.connectors.feishu.streaming;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * 飞书 connector 流式投递节流策略 — 规则全部数据驱动，便于配置覆盖。
 *
 * @param firstByteWindow      首字节加速窗口（默认 5s）
 * @param firstByteInterval    首字节窗口内最小 flush 间隔（默认 200ms）
 * @param steadyInterval       稳态最小 flush 间隔（默认 1500ms）
 * @param backoffStages        429 时各阶段退避时长（默认 [1s, 2s, 4s]）
 * @param idleThreshold        rid 视为结束的 idle 阈值（默认 30s）
 *
 * @author zsg
 * @since 2026-04-28
 */
public record FeishuStreamingPolicy(
        Duration firstByteWindow,
        Duration firstByteInterval,
        Duration steadyInterval,
        List<Duration> backoffStages,
        Duration idleThreshold
) {
    public static FeishuStreamingPolicy defaults() {
        return new FeishuStreamingPolicy(
                Duration.ofSeconds(5),
                Duration.ofMillis(200),
                Duration.ofMillis(1500),
                List.of(Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(4)),
                Duration.ofSeconds(30)
        );
    }

    public Duration minInterval(Instant firstSeen, Instant now) {
        return Duration.between(firstSeen, now).compareTo(firstByteWindow) < 0
                ? firstByteInterval : steadyInterval;
    }

    public Duration backoffDuration(int stage) {
        if (stage < 0) return Duration.ZERO;
        return backoffStages.get(Math.min(stage, backoffStages.size() - 1));
    }

    public boolean isIdle(Instant lastEnqueue, Instant now) {
        return Duration.between(lastEnqueue, now).compareTo(idleThreshold) > 0;
    }
}
