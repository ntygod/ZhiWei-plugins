package com.lifepilot.connectors.feishu.service;

import java.util.function.Function;

/**
 * 飞书平台会话工厂。
 *
 * @author zsg
 * @since 2026-03-29
 */
public interface FeishuPlatformSessionFactory {

    FeishuPlatformSession create(FeishuInstanceSettings settings,
                                 Function<FeishuInboundMessage, FeishuInboundHandlingResult> inboundHandler);
}
