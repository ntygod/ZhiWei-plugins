package com.lifepilot.connectors.qq.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lifepilot.connectors.qq.model.ConnectorDeliveryRequest;
import com.lifepilot.connectors.qq.model.ConnectorEventRequest;
import com.lifepilot.connectors.qq.model.ConnectorEventResponse;
import com.lifepilot.connectors.qq.model.ConnectorInstanceCommandRequest;
import com.lifepilot.connectors.qq.qq.QqBotApiClient;
import com.lifepilot.connectors.qq.qq.QqEventHandler;
import com.lifepilot.connectors.qq.qq.QqMessageSender;
import com.lifepilot.connectors.qq.qq.QqWebSocketManager;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * QQ connector 编排服务，管理实例生命周期。
 *
 * <p>核心职责：
 * <ul>
 *   <li>实例创建、启动、停止、重载</li>
 *   <li>健康状态查询（含 WebSocket 连接详情）</li>
 *   <li>消息投递（含错误计数和异常捕获）</li>
 *   <li>定时心跳转发到知微主服务</li>
 *   <li>进程关闭时优雅清理所有实例</li>
 * </ul>
 *
 * @author zsg
 * @since 2026-04-03
 */
@Service
public class QqConnectorService {

    private static final Logger log = LoggerFactory.getLogger(QqConnectorService.class);

    /** 心跳转发间隔（秒）。 */
    private static final long HEARTBEAT_INTERVAL_SECONDS = 60;

    private final ConcurrentHashMap<String, QqInstanceState> instances = new ConcurrentHashMap<>();
    private final ConnectorCallbackClient callbackClient;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService heartbeatScheduler;

    public QqConnectorService(ConnectorCallbackClient callbackClient, ObjectMapper objectMapper) {
        this.callbackClient = callbackClient;
        this.objectMapper = objectMapper;
        this.heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofVirtual().name("qq-heartbeat-forwarder").factory()
        );
        // 启动定时心跳转发
        heartbeatScheduler.scheduleAtFixedRate(
                this::forwardAllHeartbeats,
                HEARTBEAT_INTERVAL_SECONDS,
                HEARTBEAT_INTERVAL_SECONDS,
                TimeUnit.SECONDS
        );
    }

    /** 进程关闭时优雅停止所有实例。 */
    @PreDestroy
    public void destroy() {
        log.info("QQ connector 正在关闭，停止所有实例...");
        heartbeatScheduler.shutdownNow();
        instances.forEach((id, state) -> {
            try {
                state.shutdown();
                log.info("实例已停止: {}", id);
            } catch (Exception e) {
                log.error("实例停止失败: {}", id, e);
            }
        });
        instances.clear();
    }

    public boolean hasInstance(String instanceId) {
        return instances.containsKey(instanceId);
    }

    public boolean matchesToken(String instanceId, String runtimeToken) {
        QqInstanceState state = instances.get(instanceId);
        return state != null && state.runtimeToken.equals(runtimeToken);
    }

    // ── 实例生命周期 ────────────────────────────────────────────

    /** 启动 QQ 机器人实例。 */
    public Map<String, Object> start(String instanceId,
                                     String runtimeToken,
                                     ConnectorInstanceCommandRequest request) {
        QqInstanceState state = instances.compute(instanceId, (key, existing) -> {
            if (existing != null) {
                existing.shutdown();
            }
            return createInstance(instanceId, runtimeToken, request);
        });
        state.connect();
        log.info("QQ 实例已启动: instanceId={}, appId={}", instanceId, maskAppId(state));
        return state.commandResult("RUNNING", "QQ connector 已启动");
    }

    /** 停止 QQ 机器人实例并从管理列表移除。 */
    public Map<String, Object> stop(String instanceId) {
        QqInstanceState state = requireState(instanceId);
        state.shutdown();
        instances.remove(instanceId);
        log.info("QQ 实例已停止: instanceId={}", instanceId);
        return state.commandResult("STOPPED", "QQ connector 已停止");
    }

    /** 重载 QQ 机器人实例配置。 */
    public Map<String, Object> reload(String instanceId,
                                      ConnectorInstanceCommandRequest request) {
        QqInstanceState existing = requireState(instanceId);
        existing.shutdown();
        QqInstanceState newState = createInstance(instanceId, existing.runtimeToken, request);
        instances.put(instanceId, newState);
        newState.connect();
        log.info("QQ 实例已重载: instanceId={}", instanceId);
        return newState.commandResult("RUNNING", "QQ connector 已重载");
    }

    // ── 健康检查 ────────────────────────────────────────────────

    /** 查询实例健康状态。 */
    public Map<String, Object> health(String instanceId) {
        return requireState(instanceId).healthResult();
    }

    // ── 消息投递 ────────────────────────────────────────────────

    /** 投递消息到 QQ。 */
    public Map<String, Object> deliver(String instanceId,
                                       ConnectorDeliveryRequest request) {
        QqInstanceState state = requireState(instanceId);
        try {
            Map<String, Object> result = state.messageSender.send(request);
            state.deliveryCount.incrementAndGet();
            state.lastDeliveredAt = Instant.now();

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("accepted", true);
            payload.put("instanceId", instanceId);
            payload.put("responseId", request.responseId());
            payload.put("deliveryCount", state.deliveryCount.get());
            payload.put("deliveredAt", state.lastDeliveredAt);
            payload.put("apiResponse", result);
            return Map.copyOf(payload);
        } catch (Exception e) {
            state.errorCount.incrementAndGet();
            state.lastError = e.getMessage();
            state.lastErrorAt = Instant.now();
            log.error("消息投递失败: instanceId={}, responseId={}", instanceId, request.responseId(), e);

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("accepted", false);
            payload.put("instanceId", instanceId);
            payload.put("responseId", request.responseId());
            payload.put("error", e.getMessage());
            return Map.copyOf(payload);
        }
    }

    // ── 心跳 ────────────────────────────────────────────────────

    /** 转发单个实例心跳到知微主服务。 */
    public Map<String, Object> forwardHeartbeat(String instanceId) {
        QqInstanceState state = requireState(instanceId);
        return doForwardHeartbeat(state);
    }

    /** 定时转发所有运行中实例的心跳。 */
    private void forwardAllHeartbeats() {
        instances.forEach((instanceId, state) -> {
            if (state.wsManager.isConnected()) {
                try {
                    doForwardHeartbeat(state);
                } catch (Exception e) {
                    log.warn("心跳转发失败: instanceId={}", instanceId, e);
                }
            }
        });
    }

    private Map<String, Object> doForwardHeartbeat(QqInstanceState state) {
        Map<String, Object> response = callbackClient.forwardHeartbeat(state.instanceId, state.runtimeToken);
        state.lastHeartbeatForwardedAt = Instant.now();
        return response != null ? response : Map.of();
    }

    // ── 内部方法 ────────────────────────────────────────────────

    private QqInstanceState createInstance(String instanceId,
                                           String runtimeToken,
                                           ConnectorInstanceCommandRequest request) {
        Map<String, Object> config = request.config();
        Map<String, Object> secretConfig = request.secretConfig() != null ? request.secretConfig() : Map.of();

        String appId = stringValue(secretConfig, "appId", stringValue(config, "appId", null));
        String appSecret = stringValue(secretConfig, "appSecret", stringValue(config, "appSecret", null));

        if (appId == null || appId.isBlank() || appSecret == null || appSecret.isBlank()) {
            throw new IllegalArgumentException("QQ 机器人配置缺少 appId 或 appSecret");
        }

        QqBotApiClient apiClient = new QqBotApiClient(appId, appSecret, objectMapper);
        QqEventHandler eventHandler = new QqEventHandler(instanceId);
        QqMessageSender messageSender = new QqMessageSender(apiClient);

        QqWebSocketManager wsManager = new QqWebSocketManager(apiClient, objectMapper, event -> {
            ConnectorEventRequest eventRequest = eventHandler.convert(event);
            if (eventRequest != null) {
                try {
                    ConnectorEventResponse response = callbackClient.submitEvent(instanceId, runtimeToken, eventRequest);
                    QqInstanceState currentState = instances.get(instanceId);
                    if (currentState != null) {
                        currentState.inboundCount.incrementAndGet();
                    }
                    log.debug("事件已提交至主服务: eventType={}, responseId={}", event.type(), response.responseId());
                } catch (Exception e) {
                    QqInstanceState currentState = instances.get(instanceId);
                    if (currentState != null) {
                        currentState.errorCount.incrementAndGet();
                        currentState.lastError = "事件提交失败: " + e.getMessage();
                        currentState.lastErrorAt = Instant.now();
                    }
                    log.error("事件提交至主服务失败: eventType={}", event.type(), e);
                }
            }
        });

        return new QqInstanceState(
                instanceId, runtimeToken, request.pluginId(), request.platform(),
                apiClient, wsManager, eventHandler, messageSender
        );
    }

    private QqInstanceState requireState(String instanceId) {
        QqInstanceState state = instances.get(instanceId);
        if (state == null) {
            throw new IllegalArgumentException("实例尚未启动: " + instanceId);
        }
        return state;
    }

    private String stringValue(Map<String, Object> map, String key, @Nullable String defaultValue) {
        Object value = map.get(key);
        return value != null ? value.toString() : defaultValue;
    }

    private String maskAppId(QqInstanceState state) {
        // 日志中对 appId 做部分脱敏
        Map<String, Object> config = Map.of(); // 不直接暴露
        return state.pluginId + ":" + state.instanceId;
    }

    // ── 实例状态 ────────────────────────────────────────────────

    static final class QqInstanceState {

        final String instanceId;
        final String runtimeToken;
        final String pluginId;
        final String platform;
        final QqBotApiClient apiClient;
        final QqWebSocketManager wsManager;
        final QqEventHandler eventHandler;
        final QqMessageSender messageSender;
        final AtomicLong inboundCount = new AtomicLong();
        final AtomicLong deliveryCount = new AtomicLong();
        final AtomicLong errorCount = new AtomicLong();
        volatile Instant startedAt;
        @Nullable volatile Instant lastDeliveredAt;
        @Nullable volatile Instant lastHeartbeatForwardedAt;
        @Nullable volatile Instant lastErrorAt;
        @Nullable volatile String lastError;

        QqInstanceState(String instanceId, String runtimeToken, String pluginId, String platform,
                         QqBotApiClient apiClient, QqWebSocketManager wsManager,
                         QqEventHandler eventHandler, QqMessageSender messageSender) {
            this.instanceId = instanceId;
            this.runtimeToken = runtimeToken;
            this.pluginId = pluginId;
            this.platform = platform;
            this.apiClient = apiClient;
            this.wsManager = wsManager;
            this.eventHandler = eventHandler;
            this.messageSender = messageSender;
        }

        void connect() {
            apiClient.startTokenRefresh();
            wsManager.connect();
            startedAt = Instant.now();
        }

        void shutdown() {
            wsManager.disconnect();
            apiClient.shutdown();
        }

        Map<String, Object> commandResult(String status, String message) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("instanceId", instanceId);
            payload.put("status", status);
            payload.put("message", message);
            payload.put("platform", platform);
            payload.put("pluginId", pluginId);
            return Map.copyOf(payload);
        }

        Map<String, Object> healthResult() {
            QqWebSocketManager.ConnectionState connState = wsManager.connectionState();
            boolean healthy = connState == QqWebSocketManager.ConnectionState.CONNECTED;

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("healthy", healthy);
            payload.put("status", connState.name());
            payload.put("instanceId", instanceId);
            payload.put("platform", platform);
            payload.put("pluginId", pluginId);

            // 连接详情
            payload.put("connectedDurationSeconds", wsManager.connectedDurationSeconds());
            payload.put("totalWsEventsReceived", wsManager.totalEventsReceived());
            payload.put("totalWsReconnects", wsManager.totalReconnects());

            // Token 状态
            if (apiClient.tokenAcquiredAt() != null) {
                payload.put("tokenAcquiredAt", apiClient.tokenAcquiredAt());
                payload.put("tokenExpiresAt", apiClient.tokenExpiresAt());
            }

            // 业务计数
            payload.put("inboundCount", inboundCount.get());
            payload.put("deliveryCount", deliveryCount.get());
            payload.put("errorCount", errorCount.get());

            // 时间戳
            if (startedAt != null) payload.put("startedAt", startedAt);
            if (lastDeliveredAt != null) payload.put("lastDeliveredAt", lastDeliveredAt);
            if (lastHeartbeatForwardedAt != null) payload.put("lastHeartbeatForwardedAt", lastHeartbeatForwardedAt);

            // 错误信息
            if (lastError != null) {
                payload.put("lastError", lastError);
                payload.put("lastErrorAt", lastErrorAt);
            }

            return Map.copyOf(payload);
        }
    }
}
