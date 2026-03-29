package com.lifepilot.connectors.mock.model;

import org.springframework.lang.Nullable;

import java.util.List;
import java.util.Map;

/**
 * connector 收到的统一出站投递请求。
 *
 * @author zsg
 * @since 2026-03-29
 */
public record ConnectorDeliveryRequest(
        String instanceId,
        String responseId,
        String deliveryMode,
        Target target,
        Content content,
        List<Attachment> attachments,
        Map<String, Object> metadata
) {

    public ConnectorDeliveryRequest {
        attachments = attachments != null ? List.copyOf(attachments) : List.of();
        metadata = metadata != null ? Map.copyOf(metadata) : Map.of();
    }

    public record Target(
            @Nullable String userId,
            @Nullable String sessionId,
            Map<String, Object> attributes
    ) {

        public Target {
            attributes = attributes != null ? Map.copyOf(attributes) : Map.of();
        }
    }

    public record Content(
            String type,
            String plainText,
            Map<String, Object> payload
    ) {

        public Content {
            payload = payload != null ? Map.copyOf(payload) : Map.of();
        }
    }

    public record Attachment(
            String attachmentId,
            @Nullable String fileName,
            @Nullable String mimeType,
            String base64Data,
            long size
    ) {
    }
}
