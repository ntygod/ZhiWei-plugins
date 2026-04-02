package com.lifepilot.connectors.feishu.model;

import org.springframework.lang.Nullable;

import java.util.List;
import java.util.Map;

/**
 * 知微主服务返回的运行时响应。
 *
 * @author zsg
 * @since 2026-03-29
 */
public record ConnectorEventResponse(
        boolean accepted,
        @Nullable String responseId,
        int statusCode,
        @Nullable String errorMessage,
        List<Map<String, Object>> deliveries
) {

    public ConnectorEventResponse {
        deliveries = deliveries != null ? List.copyOf(deliveries) : List.of();
    }
}
