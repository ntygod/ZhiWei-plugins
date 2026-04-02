package com.lifepilot.connectors.feishu.model;

import org.springframework.lang.Nullable;

import java.util.Map;

/**
 * 操作请求，用于消息更新、撤回、文件上传下载、群组管理等操作。
 *
 * @author zsg
 * @since 2026-04-02
 */
public record ConnectorOperationRequest(
        String instanceId,
        String operationId,
        String operationType,
        Map<String, Object> parameters,
        @Nullable Map<String, Object> metadata
) {

    public ConnectorOperationRequest {
        parameters = parameters != null ? Map.copyOf(parameters) : Map.of();
        metadata = metadata != null ? Map.copyOf(metadata) : null;
    }
}
