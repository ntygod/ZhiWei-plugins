package com.lifepilot.connectors.feishu.service;

import org.springframework.lang.Nullable;

/**
 * 飞书发送结果。
 *
 * @author zsg
 * @since 2026-03-29
 */
public record FeishuSendResult(
        FeishuSendRoute route,
        String msgType,
        @Nullable String messageId
) {
}
