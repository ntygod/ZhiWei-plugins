package com.lifepilot.connectors.feishu.service;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link SdkFeishuPlatformSessionFactory} 单元测试。
 *
 * @author zsg
 * @since 2026-03-29
 */
class SdkFeishuPlatformSessionFactoryTest {

    @Test
    void webhook卡片动作应转换为统一入站消息并返回toast响应() throws Exception {
        List<FeishuInboundMessage> inbox = new ArrayList<>();
        FeishuInstanceSettings settings = new FeishuInstanceSettings(
                "feishu-1",
                "feishu",
                "feishu",
                "cli_mock",
                "secret",
                "webhook",
                "verify-token",
                null,
                Map.of(),
                Map.of(),
                Map.of()
        );

        FeishuPlatformSession session = new SdkFeishuPlatformSessionFactory().create(settings, message -> {
            inbox.add(message);
            return FeishuInboundHandlingResult.accepted("success", "审批动作已受理");
        });
        FeishuWebhookResponse response = session.handleWebhook(new FeishuWebhookRequest(
                "/instances/feishu-1/webhook",
                """
                {
                  "schema": "2.0",
                  "header": {
                    "event_id": "evt-card-1",
                    "token": "verify-token",
                    "create_time": "1711711711711",
                    "event_type": "card.action.trigger",
                    "tenant_key": "tenant-1",
                    "app_id": "cli_mock"
                  },
                  "event": {
                    "operator": {
                      "tenant_key": "tenant-1",
                      "open_id": "ou_card_1",
                      "user_id": "u_card_1"
                    },
                    "token": "verify-token",
                    "action": {
                      "tag": "button",
                      "name": "approve",
                      "value": {
                        "action": "approve",
                        "label": "通过"
                      }
                    },
                    "host": "open.feishu.cn",
                    "delivery_type": "stream",
                    "context": {
                      "open_message_id": "om_card_1",
                      "open_chat_id": "oc_card_1"
                    }
                  }
                }
                """.getBytes(StandardCharsets.UTF_8),
                Map.of()
        ));

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(new String(response.body(), StandardCharsets.UTF_8)).contains("审批动作已受理");
        assertThat(inbox).hasSize(1);
        FeishuInboundMessage message = inbox.getFirst();
        assertThat(message.messageType()).isEqualTo("card-action");
        assertThat(message.messageId()).isEqualTo("om_card_1");
        assertThat(message.chatId()).isEqualTo("oc_card_1");
        assertThat(message.senderOpenId()).isEqualTo("ou_card_1");
        assertThat(message.occurredAt()).isEqualTo(Instant.ofEpochMilli(1711711711711L));
        assertThat(message.content()).contains("\"eventType\":\"card.action.trigger\"");
        assertThat(message.content()).contains("\"label\":\"通过\"");
    }
}
