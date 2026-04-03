package com.lifepilot.connectors.qq.qq;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * QQ 机器人开放平台 API 客户端。
 *
 * <p>负责 access_token 生命周期管理、WebSocket 网关发现、消息发送和富媒体上传。
 *
 * <h3>Token 刷新策略：</h3>
 * <ul>
 *   <li>首次启动立即获取 token</li>
 *   <li>按 TTL 的 80% 定时自动刷新</li>
 *   <li>API 调用遇到 401 时立即同步刷新并重试一次</li>
 *   <li>刷新失败 10 秒后自动重试</li>
 * </ul>
 *
 * <h3>API 错误码处理：</h3>
 * <p>QQ API 返回非 2xx 时，响应体为 {@code {"code": 错误码, "message": "描述"}}，
 * 本客户端解析后记录结构化日志并将完整响应体透传给调用方。</p>
 *
 * @author zsg
 * @since 2026-04-03
 */
public class QqBotApiClient {

    private static final Logger log = LoggerFactory.getLogger(QqBotApiClient.class);

    private static final String TOKEN_URL = "https://bots.qq.com/app/getAppAccessToken";
    private static final String API_BASE = "https://api.sgroup.qq.com";
    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(15);
    private static final int MAX_TRANSIENT_RETRIES = 2;

    private final String appId;
    private final String appSecret;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService refreshScheduler;

    private volatile String accessToken;
    private volatile Instant tokenExpiresAt = Instant.EPOCH;
    private volatile Instant tokenAcquiredAt;

    public QqBotApiClient(String appId, String appSecret, ObjectMapper objectMapper) {
        this.appId = appId;
        this.appSecret = appSecret;
        this.objectMapper = objectMapper;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(HTTP_TIMEOUT)
                .build();
        this.refreshScheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofVirtual().name("qq-token-refresh").factory()
        );
    }

    // ── 生命周期 ──────────────────────────────────────────────────

    /** 启动 token 自动刷新。首次立即获取，之后按 80% TTL 定时刷新。 */
    public void startTokenRefresh() {
        refreshAccessToken();
    }

    /** 关闭客户端，释放资源。 */
    public void shutdown() {
        refreshScheduler.shutdownNow();
    }

    /** 获取当前有效的 access_token。若已过期则同步刷新。 */
    public String acquireAccessToken() {
        if (accessToken == null || Instant.now().isAfter(tokenExpiresAt)) {
            refreshAccessToken();
        }
        return accessToken;
    }

    /** Token 获取时间（用于外部监控）。 */
    public Instant tokenAcquiredAt() {
        return tokenAcquiredAt;
    }

    /** Token 过期时间（用于外部监控）。 */
    public Instant tokenExpiresAt() {
        return tokenExpiresAt;
    }

    // ── WebSocket 网关 ──────────────────────────────────────────

    /** 获取 WebSocket 网关 URL。 */
    public String resolveGatewayUrl() {
        Map<String, Object> body = getJson(API_BASE + "/gateway");
        String url = (String) body.get("url");
        log.info("QQ WebSocket 网关地址: {}", url);
        return url;
    }

    // ── 消息发送 ────────────────────────────────────────────────

    /**
     * 发送群消息。
     *
     * @param groupOpenId 群 openId
     * @param body        消息体
     * @return API 响应
     */
    public Map<String, Object> sendGroupMessage(String groupOpenId, Map<String, Object> body) {
        return postJson(API_BASE + "/v2/groups/" + groupOpenId + "/messages", body);
    }

    /**
     * 发送私聊消息。
     *
     * @param userOpenId 用户 openId
     * @param body       消息体
     * @return API 响应
     */
    public Map<String, Object> sendC2cMessage(String userOpenId, Map<String, Object> body) {
        return postJson(API_BASE + "/v2/users/" + userOpenId + "/messages", body);
    }

    // ── 富媒体上传 ──────────────────────────────────────────────

    /**
     * 上传群富媒体文件（图片/视频/语音/文件）。
     *
     * <p>QQ 要求先上传获得 {@code file_info}，再在消息体中引用。
     *
     * @param groupOpenId 群 openId
     * @param fileType    文件类型：1=图片, 2=视频, 3=语音, 4=文件
     * @param url         文件 URL
     * @param srvSendMsg  是否由服务端直接发送（false = 仅上传返回 file_info）
     * @return API 响应，包含 {@code file_info} 字段
     */
    public Map<String, Object> uploadGroupFile(String groupOpenId, int fileType, String url, boolean srvSendMsg) {
        Map<String, Object> body = Map.of(
                "file_type", fileType,
                "url", url,
                "srv_send_msg", srvSendMsg
        );
        return postJson(API_BASE + "/v2/groups/" + groupOpenId + "/files", body);
    }

    /**
     * 上传私聊富媒体文件。
     *
     * @param userOpenId 用户 openId
     * @param fileType   文件类型：1=图片, 2=视频, 3=语音, 4=文件
     * @param url        文件 URL
     * @param srvSendMsg 是否由服务端直接发送
     * @return API 响应，包含 {@code file_info} 字段
     */
    public Map<String, Object> uploadC2cFile(String userOpenId, int fileType, String url, boolean srvSendMsg) {
        Map<String, Object> body = Map.of(
                "file_type", fileType,
                "url", url,
                "srv_send_msg", srvSendMsg
        );
        return postJson(API_BASE + "/v2/users/" + userOpenId + "/files", body);
    }

    // ── Token 刷新 ──────────────────────────────────────────────

    private synchronized void refreshAccessToken() {
        try {
            String requestBody = objectMapper.writeValueAsString(Map.of(
                    "appId", appId,
                    "clientSecret", appSecret
            ));
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(TOKEN_URL))
                    .header("Content-Type", "application/json")
                    .timeout(HTTP_TIMEOUT)
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                log.error("QQ access_token 获取失败: HTTP {} {}", response.statusCode(), response.body());
                throw new IOException("access_token 获取失败: HTTP " + response.statusCode());
            }
            Map<String, Object> body = objectMapper.readValue(response.body(), new TypeReference<>() {});
            this.accessToken = (String) body.get("access_token");
            int expiresIn = Integer.parseInt(String.valueOf(body.get("expires_in")));
            this.tokenAcquiredAt = Instant.now();
            this.tokenExpiresAt = tokenAcquiredAt.plusSeconds(expiresIn);

            // 在 80% TTL 时安排下次刷新
            long refreshDelay = (long) (expiresIn * 0.8);
            refreshScheduler.schedule(this::refreshAccessToken, refreshDelay, TimeUnit.SECONDS);
            log.info("QQ access_token 已获取，有效期 {}s，将在 {}s 后刷新", expiresIn, refreshDelay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("QQ access_token 刷新被中断", e);
        } catch (IOException e) {
            log.error("QQ access_token 刷新失败", e);
            // 10 秒后重试
            refreshScheduler.schedule(this::refreshAccessToken, 10, TimeUnit.SECONDS);
        }
    }

    // ── HTTP 基础方法 ───────────────────────────────────────────

    private Map<String, Object> getJson(String url) {
        return executeWithRetry("GET", url, null);
    }

    private Map<String, Object> postJson(String url, Map<String, Object> body) {
        return executeWithRetry("POST", url, body);
    }

    /**
     * 执行 HTTP 请求，遇到瞬时错误（429/5xx）自动重试，遇到 401 刷新 token 重试。
     */
    private Map<String, Object> executeWithRetry(String method, String url, Map<String, Object> body) {
        IOException lastException = null;
        for (int attempt = 0; attempt <= MAX_TRANSIENT_RETRIES; attempt++) {
            try {
                HttpResponse<String> response = doHttp(method, url, body);

                // 401: 刷新 token 后重试一次（仅首次）
                if (response.statusCode() == 401 && attempt == 0) {
                    log.warn("QQ API 返回 401，刷新 token 后重试: {}", url);
                    refreshAccessToken();
                    continue;
                }

                // 429 / 5xx: 短暂等待后重试
                if ((response.statusCode() == 429 || response.statusCode() >= 500) && attempt < MAX_TRANSIENT_RETRIES) {
                    long waitMs = parseRetryAfter(response, attempt);
                    log.warn("QQ API 返回 {}，{}ms 后重试 ({}/{}): {}", response.statusCode(), waitMs, attempt + 1, MAX_TRANSIENT_RETRIES, url);
                    Thread.sleep(waitMs);
                    continue;
                }

                // 解析响应
                Map<String, Object> result = objectMapper.readValue(response.body(), new TypeReference<>() {});

                // 非 2xx 打印结构化错误日志
                if (response.statusCode() < 200 || response.statusCode() >= 300) {
                    Object code = result.get("code");
                    Object message = result.get("message");
                    log.error("QQ API 错误: {} → HTTP {} | code={} message={}", url, response.statusCode(), code, message);
                }
                return result;

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("QQ API 调用被中断: " + url, e);
            } catch (IOException e) {
                lastException = e;
                if (attempt < MAX_TRANSIENT_RETRIES) {
                    log.warn("QQ API 网络异常，重试 ({}/{}): {} — {}", attempt + 1, MAX_TRANSIENT_RETRIES, url, e.getMessage());
                }
            }
        }
        throw new RuntimeException("QQ API 调用失败（已重试 " + MAX_TRANSIENT_RETRIES + " 次）: " + url, lastException);
    }

    private HttpResponse<String> doHttp(String method, String url, Map<String, Object> body)
            throws IOException, InterruptedException {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "QQBot " + acquireAccessToken())
                .header("Content-Type", "application/json")
                .timeout(HTTP_TIMEOUT);

        if ("GET".equals(method)) {
            builder.GET();
        } else {
            String json = body != null ? objectMapper.writeValueAsString(body) : "{}";
            builder.POST(HttpRequest.BodyPublishers.ofString(json));
        }

        return httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    /**
     * 从 Retry-After 头或默认指数退避计算等待时间。
     */
    private long parseRetryAfter(HttpResponse<String> response, int attempt) {
        String retryAfter = response.headers().firstValue("Retry-After").orElse(null);
        if (retryAfter != null) {
            try {
                return Long.parseLong(retryAfter) * 1000;
            } catch (NumberFormatException ignored) {}
        }
        return (long) Math.pow(2, attempt) * 1000;
    }
}
