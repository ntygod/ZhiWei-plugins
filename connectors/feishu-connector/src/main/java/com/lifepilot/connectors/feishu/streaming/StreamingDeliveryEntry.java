package com.lifepilot.connectors.feishu.streaming;

import java.time.Instant;

/**
 * 流式投递队列条目 — 不可变快照，每次 enqueue 用 builder 模式生成新实例。
 *
 * @param instanceId      飞书 connector 实例 id
 * @param responseId      主服务侧 stable id（同一 turn 内多次 deliver 共享）
 * @param latestAccText   截至当前的最新 accText 快照
 * @param firstSeenAt     首次入队时间（用于 first-byte window 判断）
 * @param lastEnqueueAt   最近一次入队时间（用于 idle 判断）
 * @param lastFlushAt     最近一次 flush 落地时间（null 表示从未成功 flush）
 * @param backoffStage    退避阶段（0=未退避；1/2/3 对应 backoffStages 索引）
 *
 * @author zsg
 * @since 2026-04-28
 */
public record StreamingDeliveryEntry(
        String instanceId,
        String responseId,
        String latestAccText,
        Instant firstSeenAt,
        Instant lastEnqueueAt,
        Instant lastFlushAt,
        int backoffStage
) {
    public StreamingDeliveryEntry withText(String newText, Instant now) {
        return new StreamingDeliveryEntry(instanceId, responseId, newText,
                firstSeenAt, now, lastFlushAt, backoffStage);
    }

    public StreamingDeliveryEntry withFlushedAt(Instant flushAt) {
        return new StreamingDeliveryEntry(instanceId, responseId, latestAccText,
                firstSeenAt, lastEnqueueAt, flushAt, 0);
    }

    public StreamingDeliveryEntry withBackoff(int stage) {
        return new StreamingDeliveryEntry(instanceId, responseId, latestAccText,
                firstSeenAt, lastEnqueueAt, lastFlushAt, stage);
    }
}
