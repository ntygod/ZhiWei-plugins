package com.lifepilot.connectors.feishu.streaming;

/**
 * 飞书 message.update / send API 的归一化结果。
 *
 * <ul>
 *   <li>{@link #OK} — 200 成功</li>
 *   <li>{@link #RATE_LIMITED} — 429 触发退避</li>
 *   <li>{@link #NOT_FOUND} — 404/410 消息撤回，下次降级 send</li>
 *   <li>{@link #TRANSIENT} — 5xx 临时故障，下次重试</li>
 * </ul>
 *
 * @author zsg
 * @since 2026-04-28
 */
public enum FlushOutcome {
    OK, RATE_LIMITED, NOT_FOUND, TRANSIENT
}
