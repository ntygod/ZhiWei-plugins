package com.lifepilot.connectors.feishu.service;

import org.springframework.lang.Nullable;

/**
 * 飞书入站事件处理结果。
 *
 * @author zsg
 * @since 2026-03-29
 */
public record FeishuInboundHandlingResult(
        boolean accepted,
        @Nullable String toastType,
        @Nullable String toastContent
) {

    public static FeishuInboundHandlingResult accepted(@Nullable String toastType,
                                                       @Nullable String toastContent) {
        return new FeishuInboundHandlingResult(true, toastType, toastContent);
    }

    public static FeishuInboundHandlingResult ignored(@Nullable String toastType,
                                                      @Nullable String toastContent) {
        return new FeishuInboundHandlingResult(false, toastType, toastContent);
    }
}
