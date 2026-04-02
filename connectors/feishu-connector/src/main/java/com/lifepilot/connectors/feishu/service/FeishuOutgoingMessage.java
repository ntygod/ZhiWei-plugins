package com.lifepilot.connectors.feishu.service;

import org.springframework.lang.Nullable;

/**
 * 飞书出站消息。
 *
 * @author zsg
 * @since 2026-03-29
 */
public record FeishuOutgoingMessage(
        FeishuSendRoute route,
        @Nullable String targetMessageId,
        @Nullable String receiveId,
        String receiveIdType,
        String msgType,
        String content,
        boolean replyInThread,
        String uuid,
        String plainText
) {
}
