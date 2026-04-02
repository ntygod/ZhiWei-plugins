package com.lifepilot.connectors.feishu.service;

/**
 * 已发送飞书消息引用。
 *
 * @author zsg
 * @since 2026-03-29
 */
public record FeishuSentMessageRef(
        String messageId,
        String msgType
) {
}
