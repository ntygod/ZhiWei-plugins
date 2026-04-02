package com.lifepilot.connectors.feishu.service;

/**
 * 飞书渲染后的消息体。
 *
 * @author zsg
 * @since 2026-03-29
 */
public record FeishuRenderedMessage(
        String msgType,
        String content,
        String plainText
) {
}
