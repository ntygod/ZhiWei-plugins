package com.lifepilot.connectors.feishu.service;

import java.util.List;
import java.util.Map;

/**
 * 飞书 webhook 原始请求快照。
 *
 * @author zsg
 * @since 2026-03-29
 */
public record FeishuWebhookRequest(
        String httpPath,
        byte[] body,
        Map<String, List<String>> headers
) {

    public FeishuWebhookRequest {
        body = body != null ? body.clone() : new byte[0];
        headers = headers != null ? Map.copyOf(headers) : Map.of();
    }
}
