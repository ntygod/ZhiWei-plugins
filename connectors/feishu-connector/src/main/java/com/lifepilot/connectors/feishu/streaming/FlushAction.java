package com.lifepilot.connectors.feishu.streaming;

/**
 * 流式投递实际执行动作 — 由 {@link StreamingDeliveryFlushScheduler} 调用。
 *
 * <p>外部可注入 mock（测试用）或真飞书 SDK 实现（生产用，{@code FeishuFlushAction}）。</p>
 *
 * @author zsg
 * @since 2026-04-28
 */
@FunctionalInterface
public interface FlushAction {
    FlushOutcome flush(String instanceId, String responseId, String accText);
}
