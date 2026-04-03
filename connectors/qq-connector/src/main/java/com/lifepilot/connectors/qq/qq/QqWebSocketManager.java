package com.lifepilot.connectors.qq.qq;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * QQ 机器人 WebSocket 连接管理器。
 *
 * <p>处理与 QQ 网关的 WebSocket 连接生命周期，包括鉴权、心跳、事件分发和断线重连。
 * 支持心跳 ACK 超时检测——连续 {@value #MAX_MISSED_HEARTBEAT_ACKS} 次未收到 ACK 即触发重连。
 *
 * <h3>连接状态流转：</h3>
 * <pre>
 *   DISCONNECTED → CONNECTING → IDENTIFYING → CONNECTED
 *                                    ↑              ↓
 *                              RESUMING ←── (断线)
 * </pre>
 *
 * <h3>QQ WebSocket 协议 Opcode：</h3>
 * <ul>
 *   <li>0 - Dispatch：服务端推送事件</li>
 *   <li>1 - Heartbeat：客户端心跳</li>
 *   <li>2 - Identify：客户端鉴权</li>
 *   <li>6 - Resume：客户端恢复连接</li>
 *   <li>7 - Reconnect：服务端要求重连</li>
 *   <li>9 - Invalid Session：会话无效</li>
 *   <li>10 - Hello：服务端连接确认</li>
 *   <li>11 - Heartbeat ACK：心跳确认</li>
 * </ul>
 *
 * @author zsg
 * @since 2026-04-03
 */
public class QqWebSocketManager {

    private static final Logger log = LoggerFactory.getLogger(QqWebSocketManager.class);

    /** GROUP_AND_C2C_EVENT intent = 1 << 25 */
    private static final int INTENTS_GROUP_AND_C2C = 1 << 25;

    private static final int OP_DISPATCH = 0;
    private static final int OP_HEARTBEAT = 1;
    private static final int OP_IDENTIFY = 2;
    private static final int OP_RESUME = 6;
    private static final int OP_RECONNECT = 7;
    private static final int OP_INVALID_SESSION = 9;
    private static final int OP_HELLO = 10;
    private static final int OP_HEARTBEAT_ACK = 11;

    private static final long MAX_RECONNECT_DELAY_MS = 60_000;
    /** 连续未收到心跳 ACK 的容忍次数 */
    private static final int MAX_MISSED_HEARTBEAT_ACKS = 3;

    private final QqBotApiClient apiClient;
    private final ObjectMapper objectMapper;
    private final Consumer<QqDispatchEvent> eventConsumer;
    private final ScheduledExecutorService scheduler;
    private final HttpClient wsHttpClient;

    private volatile WebSocket webSocket;
    private volatile String sessionId;
    private volatile int lastSeq;
    private volatile ConnectionState state = ConnectionState.DISCONNECTED;
    private volatile ScheduledFuture<?> heartbeatTask;
    private volatile Instant connectedSince;

    private final AtomicInteger reconnectAttempts = new AtomicInteger(0);
    private final AtomicInteger missedHeartbeatAcks = new AtomicInteger(0);
    private final AtomicLong totalEventsReceived = new AtomicLong(0);
    private final AtomicLong totalReconnects = new AtomicLong(0);

    /** 连接状态枚举。 */
    public enum ConnectionState {
        DISCONNECTED, CONNECTING, IDENTIFYING, RESUMING, CONNECTED
    }

    public QqWebSocketManager(QqBotApiClient apiClient,
                              ObjectMapper objectMapper,
                              Consumer<QqDispatchEvent> eventConsumer) {
        this.apiClient = apiClient;
        this.objectMapper = objectMapper;
        this.eventConsumer = eventConsumer;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofVirtual().name("qq-ws-scheduler").factory()
        );
        this.wsHttpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(15))
                .build();
    }

    // ── 公开接口 ────────────────────────────────────────────────

    /** 建立 WebSocket 连接并开始接收事件。 */
    public void connect() {
        state = ConnectionState.CONNECTING;
        doConnect();
    }

    /** 关闭连接并释放资源。 */
    public void disconnect() {
        state = ConnectionState.DISCONNECTED;
        stopHeartbeat();
        scheduler.shutdownNow();
        WebSocket ws = this.webSocket;
        if (ws != null) {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "connector 主动关闭");
        }
        connectedSince = null;
    }

    /** 当前连接状态。 */
    public ConnectionState connectionState() {
        return state;
    }

    /** 是否处于已连接且已鉴权状态。 */
    public boolean isConnected() {
        return state == ConnectionState.CONNECTED && webSocket != null;
    }

    /** 已连接时长（秒），未连接返回 0。 */
    public long connectedDurationSeconds() {
        Instant since = connectedSince;
        return since != null && state == ConnectionState.CONNECTED
                ? Duration.between(since, Instant.now()).toSeconds() : 0;
    }

    /** 累计收到的事件数。 */
    public long totalEventsReceived() {
        return totalEventsReceived.get();
    }

    /** 累计重连次数。 */
    public long totalReconnects() {
        return totalReconnects.get();
    }

    // ── 连接管理 ────────────────────────────────────────────────

    private void doConnect() {
        if (state == ConnectionState.DISCONNECTED) return;
        state = ConnectionState.CONNECTING;
        try {
            String gatewayUrl = apiClient.resolveGatewayUrl();
            log.info("正在连接 QQ WebSocket 网关: {}", gatewayUrl);
            wsHttpClient.newWebSocketBuilder()
                    .connectTimeout(Duration.ofSeconds(15))
                    .buildAsync(URI.create(gatewayUrl), new QqWebSocketListener())
                    .thenAccept(ws -> {
                        this.webSocket = ws;
                        reconnectAttempts.set(0);
                        log.info("QQ WebSocket 连接已建立");
                    })
                    .exceptionally(ex -> {
                        log.error("QQ WebSocket 连接失败", ex);
                        scheduleReconnect();
                        return null;
                    });
        } catch (Exception e) {
            log.error("QQ WebSocket 连接异常", e);
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        if (state == ConnectionState.DISCONNECTED) return;
        int attempt = reconnectAttempts.incrementAndGet();
        totalReconnects.incrementAndGet();
        long delay = Math.min((long) Math.pow(2, attempt - 1) * 1000, MAX_RECONNECT_DELAY_MS);
        log.info("将在 {}ms 后进行第 {} 次重连", delay, attempt);
        scheduler.schedule(this::doConnect, delay, TimeUnit.MILLISECONDS);
    }

    // ── 协议消息发送 ────────────────────────────────────────────

    private void sendIdentify() {
        state = ConnectionState.IDENTIFYING;
        String token = "QQBot " + apiClient.acquireAccessToken();
        Map<String, Object> payload = Map.of(
                "op", OP_IDENTIFY,
                "d", Map.of(
                        "token", token,
                        "intents", INTENTS_GROUP_AND_C2C,
                        "shard", new int[]{0, 1}
                )
        );
        sendJson(payload);
        log.info("已发送 Identify 请求");
    }

    private void sendResume() {
        state = ConnectionState.RESUMING;
        String token = "QQBot " + apiClient.acquireAccessToken();
        Map<String, Object> payload = Map.of(
                "op", OP_RESUME,
                "d", Map.of(
                        "token", token,
                        "session_id", sessionId,
                        "seq", lastSeq
                )
        );
        sendJson(payload);
        log.info("已发送 Resume 请求: sessionId={}, seq={}", sessionId, lastSeq);
    }

    private void sendHeartbeat() {
        int missed = missedHeartbeatAcks.incrementAndGet();
        if (missed > MAX_MISSED_HEARTBEAT_ACKS) {
            log.warn("连续 {} 次未收到心跳 ACK，触发重连", missed);
            closeAndReconnect();
            return;
        }
        Map<String, Object> payload = Map.of("op", OP_HEARTBEAT, "d", lastSeq);
        sendJson(payload);
        log.debug("已发送心跳: seq={}, 未确认次数={}", lastSeq, missed);
    }

    private void startHeartbeat(long intervalMs) {
        stopHeartbeat();
        missedHeartbeatAcks.set(0);
        heartbeatTask = scheduler.scheduleAtFixedRate(
                this::sendHeartbeat, intervalMs, intervalMs, TimeUnit.MILLISECONDS
        );
        log.info("心跳定时器已启动: 间隔={}ms", intervalMs);
    }

    private void stopHeartbeat() {
        ScheduledFuture<?> task = this.heartbeatTask;
        if (task != null) {
            task.cancel(false);
        }
    }

    private void sendJson(Map<String, Object> payload) {
        WebSocket ws = this.webSocket;
        if (ws == null) return;
        try {
            String json = objectMapper.writeValueAsString(payload);
            ws.sendText(json, true);
        } catch (Exception e) {
            log.error("WebSocket 发送消息失败", e);
        }
    }

    // ── 消息处理 ────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private void handleMessage(String text) {
        try {
            Map<String, Object> message = objectMapper.readValue(text, new TypeReference<>() {});
            int op = ((Number) message.get("op")).intValue();
            Object d = message.get("d");
            String t = (String) message.get("t");
            Number seqNum = (Number) message.get("s");
            if (seqNum != null) {
                lastSeq = seqNum.intValue();
            }

            switch (op) {
                case OP_HELLO -> {
                    Map<String, Object> data = (Map<String, Object>) d;
                    long heartbeatInterval = ((Number) data.get("heartbeat_interval")).longValue();
                    startHeartbeat(heartbeatInterval);
                    if (sessionId != null) {
                        sendResume();
                    } else {
                        sendIdentify();
                    }
                }
                case OP_DISPATCH -> handleDispatch(t, d);
                case OP_HEARTBEAT_ACK -> {
                    missedHeartbeatAcks.set(0);
                    log.debug("收到心跳 ACK");
                }
                case OP_RECONNECT -> {
                    log.warn("服务端要求重连");
                    closeAndReconnect();
                }
                case OP_INVALID_SESSION -> {
                    boolean resumable = Boolean.TRUE.equals(d);
                    log.warn("会话无效: resumable={}", resumable);
                    if (!resumable) {
                        sessionId = null;
                        lastSeq = 0;
                    }
                    state = ConnectionState.CONNECTING;
                    connectedSince = null;
                    // QQ 文档建议等 1-5 秒后重连
                    long delay = 1000 + (long) (Math.random() * 4000);
                    stopHeartbeat();
                    WebSocket ws = this.webSocket;
                    if (ws != null) {
                        ws.sendClose(WebSocket.NORMAL_CLOSURE, "会话无效重连");
                    }
                    scheduler.schedule(this::doConnect, delay, TimeUnit.MILLISECONDS);
                }
                default -> log.debug("忽略未知 opcode: {}", op);
            }
        } catch (Exception e) {
            log.error("处理 WebSocket 消息异常: {}", text, e);
        }
    }

    @SuppressWarnings("unchecked")
    private void handleDispatch(String t, Object d) {
        if ("READY".equals(t)) {
            Map<String, Object> data = (Map<String, Object>) d;
            sessionId = (String) data.get("session_id");
            state = ConnectionState.CONNECTED;
            connectedSince = Instant.now();
            log.info("QQ WebSocket 鉴权成功: sessionId={}", sessionId);
        } else if ("RESUMED".equals(t)) {
            state = ConnectionState.CONNECTED;
            connectedSince = Instant.now();
            log.info("QQ WebSocket 恢复成功");
        } else if (t != null) {
            totalEventsReceived.incrementAndGet();
            Map<String, Object> data = d instanceof Map ? (Map<String, Object>) d : Map.of();
            try {
                eventConsumer.accept(new QqDispatchEvent(t, data));
            } catch (Exception e) {
                log.error("事件消费异常: type={}", t, e);
            }
        }
    }

    private void closeAndReconnect() {
        state = ConnectionState.CONNECTING;
        connectedSince = null;
        stopHeartbeat();
        WebSocket ws = this.webSocket;
        if (ws != null) {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "重连");
        }
        scheduleReconnect();
    }

    // ── 内部类型 ────────────────────────────────────────────────

    /** QQ 网关下发的分发事件。 */
    public record QqDispatchEvent(String type, Map<String, Object> data) {}

    /** WebSocket 监听器实现。 */
    private class QqWebSocketListener implements WebSocket.Listener {

        private final StringBuilder textBuffer = new StringBuilder();

        @Override
        public void onOpen(WebSocket webSocket) {
            log.info("QQ WebSocket 已打开");
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            textBuffer.append(data);
            if (last) {
                String fullMessage = textBuffer.toString();
                textBuffer.setLength(0);
                handleMessage(fullMessage);
            }
            webSocket.request(1);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<?> onPing(WebSocket webSocket, ByteBuffer message) {
            webSocket.request(1);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            log.warn("QQ WebSocket 已关闭: statusCode={}, reason={}", statusCode, reason);
            connectedSince = null;
            if (state != ConnectionState.DISCONNECTED) {
                state = ConnectionState.CONNECTING;
                scheduleReconnect();
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            log.error("QQ WebSocket 错误", error);
            connectedSince = null;
            if (state != ConnectionState.DISCONNECTED) {
                state = ConnectionState.CONNECTING;
                scheduleReconnect();
            }
        }
    }
}
