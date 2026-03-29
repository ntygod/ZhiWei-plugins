package com.lifepilot.connectors.mock.service;

import com.lifepilot.connectors.mock.model.ConnectorDeliveryRequest;
import com.lifepilot.connectors.mock.model.ConnectorEventRequest;
import com.lifepilot.connectors.mock.model.ConnectorEventResponse;
import com.lifepilot.connectors.mock.model.ConnectorInstanceCommandRequest;
import com.lifepilot.connectors.mock.model.SimulatedInboundEventRequest;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * mock connector 的内存运行时。
 *
 * @author zsg
 * @since 2026-03-29
 */
@Service
public class MockConnectorService {

    private final ConcurrentHashMap<String, MockInstanceState> instances = new ConcurrentHashMap<>();
    private final ConnectorCallbackClient connectorCallbackClient;

    public MockConnectorService(ConnectorCallbackClient connectorCallbackClient) {
        this.connectorCallbackClient = connectorCallbackClient;
    }

    public boolean hasInstance(String instanceId) {
        return instances.containsKey(instanceId);
    }

    public boolean matchesToken(String instanceId, String runtimeToken) {
        MockInstanceState state = instances.get(instanceId);
        return state != null && state.runtimeToken().equals(runtimeToken);
    }

    public Map<String, Object> start(String instanceId,
                                     String runtimeToken,
                                     ConnectorInstanceCommandRequest request) {
        MockInstanceState state = instances.compute(instanceId, (key, existing) -> {
            MockInstanceState current = existing != null ? existing : new MockInstanceState(instanceId, runtimeToken);
            current.start(request);
            return current;
        });
        return state.commandResult("RUNNING", "mock connector 已启动");
    }

    public Map<String, Object> stop(String instanceId) {
        MockInstanceState state = requireState(instanceId);
        state.stop();
        return state.commandResult("STOPPED", "mock connector 已停止");
    }

    public Map<String, Object> reload(String instanceId,
                                      ConnectorInstanceCommandRequest request) {
        MockInstanceState state = requireState(instanceId);
        state.reload(request);
        return state.commandResult("RUNNING", "mock connector 已重载");
    }

    public Map<String, Object> health(String instanceId) {
        return requireState(instanceId).healthResult();
    }

    public Map<String, Object> deliver(String instanceId,
                                       ConnectorDeliveryRequest request) {
        MockInstanceState state = requireState(instanceId);
        state.recordDelivery(request);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("accepted", true);
        payload.put("instanceId", instanceId);
        payload.put("responseId", request.responseId());
        payload.put("deliveryCount", state.deliveryCount());
        payload.put("deliveredAt", state.lastDeliveredAt());
        payload.put("contentType", request.content() != null ? request.content().type() : "text");
        return Map.copyOf(payload);
    }

    public ConnectorEventResponse simulateEvent(String instanceId,
                                                SimulatedInboundEventRequest request) {
        MockInstanceState state = requireState(instanceId);
        ConnectorEventRequest eventRequest = new ConnectorEventRequest(
                UUID.randomUUID().toString(),
                null,
                request.userId(),
                request.sessionId(),
                new ConnectorEventRequest.Content("text", request.text(), null, null),
                List.of(),
                mergeMetadata(state, request.metadata()),
                new ConnectorEventRequest.DeliveryHints(request.deliveryMode()),
                new ConnectorEventRequest.ReplyTarget(request.userId(), request.sessionId(), Map.of()),
                Instant.now()
        );
        ConnectorEventResponse response = connectorCallbackClient.submitEvent(
                instanceId,
                state.runtimeToken(),
                eventRequest
        );
        state.recordSimulatedEvent(response.responseId());
        return response;
    }

    public Map<String, Object> forwardHeartbeat(String instanceId) {
        MockInstanceState state = requireState(instanceId);
        Map<String, Object> response = connectorCallbackClient.forwardHeartbeat(instanceId, state.runtimeToken());
        state.recordHeartbeat();
        return response != null ? response : Map.of();
    }

    private Map<String, Object> mergeMetadata(MockInstanceState state,
                                              @Nullable Map<String, Object> incomingMetadata) {
        LinkedHashMap<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("source", "mock-http-connector");
        metadata.put("platform", state.platform());
        metadata.put("pluginId", state.pluginId());
        if (incomingMetadata != null) {
            metadata.putAll(incomingMetadata);
        }
        return Map.copyOf(metadata);
    }

    private MockInstanceState requireState(String instanceId) {
        MockInstanceState state = instances.get(instanceId);
        if (state == null) {
            throw new IllegalArgumentException("实例尚未启动: " + instanceId);
        }
        return state;
    }

    static final class MockInstanceState {

        private final String instanceId;
        private final String runtimeToken;
        private String pluginId = "mock";
        private String platform = "mock";
        private Map<String, Object> config = Map.of();
        private Map<String, Object> secretConfig = Map.of();
        private Map<String, Object> routingPolicy = Map.of();
        private boolean running;
        private int deliveryCount;
        private int simulatedEventCount;
        @Nullable
        private Instant lastStartedAt;
        @Nullable
        private Instant lastReloadedAt;
        @Nullable
        private Instant lastDeliveredAt;
        @Nullable
        private Instant lastSimulatedEventAt;
        @Nullable
        private Instant lastHeartbeatForwardedAt;
        @Nullable
        private String lastResponseId;

        MockInstanceState(String instanceId, String runtimeToken) {
            this.instanceId = instanceId;
            this.runtimeToken = runtimeToken;
        }

        synchronized void start(ConnectorInstanceCommandRequest request) {
            pluginId = request.pluginId();
            platform = request.platform();
            config = request.config();
            secretConfig = request.secretConfig() != null ? request.secretConfig() : Map.of();
            routingPolicy = request.routingPolicy() != null ? request.routingPolicy() : Map.of();
            running = true;
            lastStartedAt = Instant.now();
        }

        synchronized void stop() {
            running = false;
        }

        synchronized void reload(ConnectorInstanceCommandRequest request) {
            pluginId = request.pluginId();
            platform = request.platform();
            config = request.config();
            secretConfig = request.secretConfig() != null ? request.secretConfig() : Map.of();
            routingPolicy = request.routingPolicy() != null ? request.routingPolicy() : Map.of();
            running = true;
            lastReloadedAt = Instant.now();
        }

        synchronized void recordDelivery(ConnectorDeliveryRequest request) {
            deliveryCount += 1;
            lastDeliveredAt = Instant.now();
            lastResponseId = request.responseId();
        }

        synchronized void recordSimulatedEvent(@Nullable String responseId) {
            simulatedEventCount += 1;
            lastSimulatedEventAt = Instant.now();
            lastResponseId = responseId;
        }

        synchronized void recordHeartbeat() {
            lastHeartbeatForwardedAt = Instant.now();
        }

        synchronized Map<String, Object> commandResult(String status, String message) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("instanceId", instanceId);
            payload.put("status", status);
            payload.put("message", message);
            payload.put("platform", platform);
            payload.put("pluginId", pluginId);
            payload.put("running", running);
            payload.put("config", config);
            return Map.copyOf(payload);
        }

        synchronized Map<String, Object> healthResult() {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("healthy", running);
            payload.put("status", running ? "RUNNING" : "STOPPED");
            payload.put("instanceId", instanceId);
            payload.put("platform", platform);
            payload.put("pluginId", pluginId);
            payload.put("deliveryCount", deliveryCount);
            payload.put("simulatedEventCount", simulatedEventCount);
            putIfNotNull(payload, "lastStartedAt", lastStartedAt);
            putIfNotNull(payload, "lastReloadedAt", lastReloadedAt);
            putIfNotNull(payload, "lastDeliveredAt", lastDeliveredAt);
            putIfNotNull(payload, "lastEventAt", lastSimulatedEventAt);
            putIfNotNull(payload, "lastHeartbeatForwardedAt", lastHeartbeatForwardedAt);
            putIfNotNull(payload, "lastResponseId", lastResponseId);
            payload.put("routingPolicy", routingPolicy);
            payload.put("secretConfigKeys", secretConfig.keySet());
            return Map.copyOf(payload);
        }

        String runtimeToken() {
            return runtimeToken;
        }

        String pluginId() {
            return pluginId;
        }

        String platform() {
            return platform;
        }

        int deliveryCount() {
            return deliveryCount;
        }

        @Nullable
        Instant lastDeliveredAt() {
            return lastDeliveredAt;
        }

        private void putIfNotNull(Map<String, Object> payload, String key, @Nullable Object value) {
            if (value != null) {
                payload.put(key, value);
            }
        }
    }
}
