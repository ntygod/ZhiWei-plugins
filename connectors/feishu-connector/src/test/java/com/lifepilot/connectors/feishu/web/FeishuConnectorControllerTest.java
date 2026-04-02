package com.lifepilot.connectors.feishu.web;

import com.lifepilot.connectors.feishu.model.ConnectorOperationRequest;
import com.lifepilot.connectors.feishu.model.ConnectorOperationResponse;
import com.lifepilot.connectors.feishu.service.FeishuConnectorService;
import com.lifepilot.connectors.feishu.service.FeishuWebhookResponse;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * {@link FeishuConnectorController} 单元测试。
 *
 * @author zsg
 * @since 2026-03-29
 */
class FeishuConnectorControllerTest {

    @Test
    void webhook接口应转发飞书原始请求并返回平台响应() throws Exception {
        FeishuConnectorService service = mock(FeishuConnectorService.class);
        when(service.handleWebhook(eq("feishu-1"), any())).thenReturn(new FeishuWebhookResponse(
                200,
                "{\"challenge\":\"abc\"}".getBytes(StandardCharsets.UTF_8),
                Map.of("Content-Type", List.of(MediaType.APPLICATION_JSON_VALUE))
        ));

        MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new FeishuConnectorController(service)).build();

        mockMvc.perform(post("/instances/feishu-1/webhook")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("x-lark-request-timestamp", "1")
                        .header("x-lark-request-nonce", "nonce")
                        .header("x-lark-signature", "signature")
                        .content("{\"type\":\"url_verification\",\"challenge\":\"abc\"}"))
                .andExpect(status().isOk())
                .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_VALUE))
                .andExpect(content().json("{\"challenge\":\"abc\"}"));

        verify(service).handleWebhook(eq("feishu-1"), any());
    }

    @Test
    void operate端点_操作成功应返回200() throws Exception {
        FeishuConnectorService service = mock(FeishuConnectorService.class);
        when(service.hasInstance("feishu-1")).thenReturn(true);
        when(service.matchesToken("feishu-1", "token-1")).thenReturn(true);
        when(service.handleOperation(eq("feishu-1"), any(ConnectorOperationRequest.class)))
                .thenReturn(ConnectorOperationResponse.success("op-1", Map.of("recalled", true)));

        MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new FeishuConnectorController(service)).build();

        mockMvc.perform(post("/instances/feishu-1/operate")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Channel-Instance-Token", "token-1")
                        .content("""
                                {
                                    "instanceId": "feishu-1",
                                    "operationId": "op-1",
                                    "operationType": "message_recall",
                                    "parameters": {"messageId": "om_1"}
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.operationId").value("op-1"))
                .andExpect(jsonPath("$.result.recalled").value(true));

        verify(service).handleOperation(eq("feishu-1"), any(ConnectorOperationRequest.class));
    }

    @Test
    void operate端点_操作失败应返回422() throws Exception {
        FeishuConnectorService service = mock(FeishuConnectorService.class);
        when(service.hasInstance("feishu-1")).thenReturn(true);
        when(service.matchesToken("feishu-1", "token-1")).thenReturn(true);
        when(service.handleOperation(eq("feishu-1"), any(ConnectorOperationRequest.class)))
                .thenReturn(ConnectorOperationResponse.failure("op-fail-1", "不支持的操作类型: unknown"));

        MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new FeishuConnectorController(service)).build();

        mockMvc.perform(post("/instances/feishu-1/operate")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Channel-Instance-Token", "token-1")
                        .content("""
                                {
                                    "instanceId": "feishu-1",
                                    "operationId": "op-fail-1",
                                    "operationType": "unknown",
                                    "parameters": {}
                                }
                                """))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.operationId").value("op-fail-1"))
                .andExpect(jsonPath("$.errorMessage").value("不支持的操作类型: unknown"));
    }
}
