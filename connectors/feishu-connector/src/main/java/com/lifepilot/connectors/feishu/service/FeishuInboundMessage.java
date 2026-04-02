package com.lifepilot.connectors.feishu.service;

import org.springframework.lang.Nullable;

import java.time.Instant;

/**
 * 飞书入站消息快照。
 *
 * @author zsg
 * @since 2026-03-29
 */
public record FeishuInboundMessage(
        @Nullable String eventId,
        @Nullable String messageId,
        String messageType,
        @Nullable String chatId,
        @Nullable String chatType,
        @Nullable String threadId,
        @Nullable String rootId,
        @Nullable String parentId,
        @Nullable String senderOpenId,
        @Nullable String senderUserId,
        @Nullable String senderUnionId,
        @Nullable String senderType,
        @Nullable String tenantKey,
        @Nullable String content,
        @Nullable Instant occurredAt
) {
}
