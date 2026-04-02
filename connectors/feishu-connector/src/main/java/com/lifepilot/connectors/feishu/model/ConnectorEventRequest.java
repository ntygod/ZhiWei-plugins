package com.lifepilot.connectors.feishu.model;

import org.springframework.lang.Nullable;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * connector 回调知微主服务的统一入站事件。
 *
 * @author zsg
 * @since 2026-03-29
 */
public record ConnectorEventRequest(
        @Nullable String eventId,
        @Nullable String messageId,
        String userId,
        @Nullable String sessionId,
        Content content,
        List<Attachment> attachments,
        @Nullable Map<String, Object> metadata,
        @Nullable DeliveryHints deliveryHints,
        @Nullable ReplyTarget replyTarget,
        @Nullable Instant occurredAt
) {

    public ConnectorEventRequest {
        attachments = attachments != null ? List.copyOf(attachments) : List.of();
        metadata = metadata != null ? Map.copyOf(metadata) : null;
    }

    public record Content(
            @Nullable String type,
            @Nullable String text,
            @Nullable String name,
            @Nullable Map<String, Object> payload
    ) {

        public Content {
            type = type != null && !type.isBlank() ? type.trim() : "text";
            payload = payload != null ? Map.copyOf(payload) : null;
        }
    }

    public record Attachment(
            @Nullable String attachmentId,
            @Nullable String fileName,
            @Nullable String mimeType,
            String base64Data,
            long size
    ) {
    }

    public record DeliveryHints(@Nullable String deliveryMode) {
    }

    public record ReplyTarget(
            @Nullable String userId,
            @Nullable String sessionId,
            @Nullable Map<String, Object> attributes
    ) {

        public ReplyTarget {
            attributes = attributes != null ? Map.copyOf(attributes) : null;
        }
    }
}
