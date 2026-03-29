package com.lifepilot.connectors.mock.service;

import com.lifepilot.connectors.mock.model.ConnectorDeliveryRequest;
import com.lifepilot.connectors.mock.model.ConnectorEventResponse;
import com.lifepilot.connectors.mock.model.ConnectorInstanceCommandRequest;
import com.lifepilot.connectors.mock.model.SimulatedInboundEventRequest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link MockConnectorService} 单元测试。
 *
 * @author zsg
 * @since 2026-03-29
 */
class MockConnectorServiceTest {

    @Test
    void start和deliver_应记录实例运行状态与投递次数() {
        ConnectorCallbackClient callbackClient = mock(ConnectorCallbackClient.class);
        MockConnectorService service = new MockConnectorService(callbackClient);

        service.start("mock-1", "runtime-token", new ConnectorInstanceCommandRequest(
                "mock-1",
                "feishu",
                "feishu",
                Map.of("baseUrl", "http://127.0.0.1:19090"),
                Map.of("runtimeToken", "runtime-token"),
                null
        ));
        Map<String, Object> deliveryResult = service.deliver("mock-1", new ConnectorDeliveryRequest(
                "mock-1",
                "resp-1",
                "ASYNC_PUSH",
                new ConnectorDeliveryRequest.Target("u-1", "s-1", Map.of()),
                new ConnectorDeliveryRequest.Content("text", "hello", Map.of("text", "hello")),
                List.of(),
                Map.of()
        ));
        Map<String, Object> health = service.health("mock-1");

        assertThat(deliveryResult).containsEntry("accepted", true);
        assertThat(deliveryResult).containsEntry("deliveryCount", 1);
        assertThat(health).containsEntry("healthy", true);
        assertThat(health).containsEntry("deliveryCount", 1);
        assertThat(health).containsEntry("platform", "feishu");
    }

    @Test
    void simulateEvent_应调用知微回调客户端并累积事件次数() {
        ConnectorCallbackClient callbackClient = mock(ConnectorCallbackClient.class);
        when(callbackClient.submitEvent(eq("mock-1"), eq("runtime-token"), any()))
                .thenReturn(new ConnectorEventResponse(true, "resp-1", 200, null, List.of()));

        MockConnectorService service = new MockConnectorService(callbackClient);
        service.start("mock-1", "runtime-token", new ConnectorInstanceCommandRequest(
                "mock-1",
                "feishu",
                "feishu",
                Map.of("baseUrl", "http://127.0.0.1:19090"),
                Map.of("runtimeToken", "runtime-token"),
                null
        ));

        ConnectorEventResponse response = service.simulateEvent("mock-1", new SimulatedInboundEventRequest(
                "u-1",
                "s-1",
                "你好，知微",
                "ASYNC_PUSH",
                Map.of("source", "test")
        ));
        Map<String, Object> health = service.health("mock-1");

        assertThat(response.accepted()).isTrue();
        assertThat(response.responseId()).isEqualTo("resp-1");
        assertThat(health).containsEntry("simulatedEventCount", 1);
        verify(callbackClient).submitEvent(eq("mock-1"), eq("runtime-token"), any());
    }
}
