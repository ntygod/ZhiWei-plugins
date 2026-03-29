package com.lifepilot.connectors.mock.model;

import org.springframework.lang.Nullable;

import java.util.Map;

/**
 * 模拟入站事件请求。
 *
 * @author zsg
 * @since 2026-03-29
 */
public record SimulatedInboundEventRequest(
        String userId,
        @Nullable String sessionId,
        String text,
        @Nullable String deliveryMode,
        @Nullable Map<String, Object> metadata
) {

    public SimulatedInboundEventRequest {
        userId = userId != null && !userId.isBlank() ? userId.trim() : "mock-user";
        text = text != null ? text : "";
        deliveryMode = deliveryMode != null && !deliveryMode.isBlank() ? deliveryMode.trim() : "ASYNC_PUSH";
        metadata = metadata != null ? Map.copyOf(metadata) : null;
    }
}
