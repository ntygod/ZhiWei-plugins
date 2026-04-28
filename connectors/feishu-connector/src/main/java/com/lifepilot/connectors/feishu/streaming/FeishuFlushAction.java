package com.lifepilot.connectors.feishu.streaming;

import com.lifepilot.connectors.feishu.service.FeishuConnectorService;

/**
 * 飞书 SDK 适配的 {@link FlushAction} 实现。
 *
 * <p>把 scheduler 的 flush 调用桥接到 {@link FeishuConnectorService#flushStreamingDelivery}，
 * 由 service 调用真实飞书 SDK 并把异常归一化为 {@link FlushOutcome}。</p>
 *
 * <p>本类自身只做兜底防御：service 层抛出未预期异常时归 {@link FlushOutcome#TRANSIENT}，
 * 让 scheduler 下次 tick 重试，避免单次异常拖垮整个流式投递循环。</p>
 *
 * @author zsg
 * @since 2026-04-28
 */
public class FeishuFlushAction implements FlushAction {

    private final FeishuConnectorService service;

    public FeishuFlushAction(FeishuConnectorService service) {
        this.service = service;
    }

    @Override
    public FlushOutcome flush(String instanceId, String responseId, String accText) {
        try {
            return service.flushStreamingDelivery(instanceId, responseId, accText);
        } catch (Exception ex) {
            // service 内部已对已知异常做映射；这里只兜底未预期异常
            return FlushOutcome.TRANSIENT;
        }
    }
}
