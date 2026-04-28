package com.lifepilot.connectors.feishu.streaming;

import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 流式投递队列 — 按 responseId 维护"最新一份 accText 快照"。
 *
 * <p>高频 enqueue 只保留最新（覆盖旧条目，无积压）；不同 responseId 互相隔离。
 * Scheduler 通过 {@link #snapshot()} 拿到所有条目快照后决定 flush。</p>
 *
 * @author zsg
 * @since 2026-04-28
 */
public class StreamingDeliveryQueue {

    private final ConcurrentHashMap<String, StreamingDeliveryEntry> entries = new ConcurrentHashMap<>();

    public void enqueue(String instanceId, String responseId, String accText, Instant now) {
        entries.compute(responseId, (rid, existing) -> {
            if (existing == null) {
                return new StreamingDeliveryEntry(instanceId, rid, accText, now, now, null, 0);
            }
            return existing.withText(accText, now);
        });
    }

    public Optional<StreamingDeliveryEntry> peek(String responseId) {
        return Optional.ofNullable(entries.get(responseId));
    }

    public Collection<StreamingDeliveryEntry> snapshot() {
        return entries.values();
    }

    public void update(StreamingDeliveryEntry next) {
        entries.put(next.responseId(), next);
    }

    public void remove(String responseId) {
        entries.remove(responseId);
    }
}
