package com.lifepilot.connectors.feishu.model;

import org.springframework.lang.Nullable;

import java.util.Map;

/**
 * 操作响应，包含操作执行结果。
 *
 * @author zsg
 * @since 2026-04-02
 */
public record ConnectorOperationResponse(
        boolean success,
        String operationId,
        Map<String, Object> result,
        @Nullable String errorMessage
) {

    public ConnectorOperationResponse {
        result = result != null ? Map.copyOf(result) : Map.of();
    }

    /**
     * 创建成功响应。
     */
    public static ConnectorOperationResponse success(String operationId, Map<String, Object> result) {
        return new ConnectorOperationResponse(true, operationId, result, null);
    }

    /**
     * 创建失败响应。
     */
    public static ConnectorOperationResponse failure(String operationId, String errorMessage) {
        return new ConnectorOperationResponse(false, operationId, Map.of(), errorMessage);
    }
}
