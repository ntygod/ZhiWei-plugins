package com.lifepilot.connectors.qq.qq;

import com.lifepilot.connectors.qq.model.ConnectorDeliveryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * QQ 消息发送器，将统一的投递请求转换为 QQ API 调用。
 *
 * <p>核心能力：
 * <ul>
 *   <li>群消息 / 私聊消息路由</li>
 *   <li>被动回复 5 分钟窗口检测，超时自动切换为主动推送</li>
 *   <li>Markdown 消息支持（msg_type=2）</li>
 *   <li>超长消息自动分段（QQ 单条消息限制约 2000 字符）</li>
 *   <li>富媒体消息支持（msg_type=7，需先上传获取 file_info）</li>
 * </ul>
 *
 * <h3>QQ 消息类型：</h3>
 * <ul>
 *   <li>0 — 文本</li>
 *   <li>2 — Markdown</li>
 *   <li>7 — 富媒体（图片/视频/语音/文件）</li>
 * </ul>
 *
 * @author zsg
 * @since 2026-04-03
 */
public class QqMessageSender {

    private static final Logger log = LoggerFactory.getLogger(QqMessageSender.class);

    /** QQ 被动回复窗口 5 分钟。 */
    private static final Duration PASSIVE_REPLY_WINDOW = Duration.ofMinutes(5);
    /** QQ 单条消息最大字符数（留 100 字符余量）。 */
    private static final int MAX_MESSAGE_LENGTH = 1900;

    private final QqBotApiClient apiClient;
    private final AtomicInteger msgSeqCounter = new AtomicInteger(1);

    public QqMessageSender(QqBotApiClient apiClient) {
        this.apiClient = apiClient;
    }

    /**
     * 根据投递请求发送消息到 QQ。支持自动分段和消息类型识别。
     *
     * @param request 统一投递请求
     * @return 最后一段消息的 QQ API 响应
     */
    public Map<String, Object> send(ConnectorDeliveryRequest request) {
        Map<String, Object> attributes = request.target().attributes();
        String msgType = (String) attributes.get("msgType");
        String msgId = (String) attributes.get("msgId");
        String receivedAt = (String) attributes.get("receivedAt");
        String text = request.content() != null ? request.content().plainText() : "";
        String contentType = request.content() != null ? request.content().type() : "text";

        // 判断是否仍在被动回复窗口内
        String effectiveMsgId = isWithinPassiveWindow(receivedAt) ? msgId : null;

        // 构建消息体列表（可能多段）
        List<Map<String, Object>> bodies = buildMessageBodies(text, contentType, effectiveMsgId);

        // 依次发送
        Map<String, Object> lastResponse = Map.of();
        for (Map<String, Object> body : bodies) {
            lastResponse = doSend(msgType, attributes, body);
        }
        return lastResponse;
    }

    // ── 消息构建 ────────────────────────────────────────────────

    /**
     * 构建消息体列表。超长文本会被拆分为多段。
     */
    private List<Map<String, Object>> buildMessageBodies(String text, String contentType, String msgId) {
        boolean isMarkdown = "markdown".equalsIgnoreCase(contentType);

        // 分段
        List<String> segments = segmentText(text, MAX_MESSAGE_LENGTH);
        List<Map<String, Object>> bodies = new ArrayList<>();

        for (int i = 0; i < segments.size(); i++) {
            String segment = segments.get(i);
            Map<String, Object> body = new LinkedHashMap<>();

            if (isMarkdown) {
                body.put("msg_type", 2);
                body.put("markdown", Map.of("content", segment));
            } else {
                body.put("msg_type", 0);
                body.put("content", segment);
            }

            // 第一段使用 msg_id（被动回复），后续段使用 msg_seq（主动推送）
            if (i == 0 && msgId != null && !msgId.isBlank()) {
                body.put("msg_id", msgId);
            } else {
                body.put("msg_seq", msgSeqCounter.getAndIncrement());
            }

            bodies.add(Map.copyOf(body));
        }

        return bodies;
    }

    /**
     * 发送富媒体消息（图片等）。需要先上传文件获取 file_info。
     *
     * @param msgType     "group" 或 "c2c"
     * @param attributes  目标属性
     * @param fileUrl     文件 URL
     * @param fileType    文件类型：1=图片, 2=视频, 3=语音, 4=文件
     * @param msgId       被动回复消息 ID（可选）
     * @return QQ API 响应
     */
    public Map<String, Object> sendMedia(String msgType, Map<String, Object> attributes,
                                          String fileUrl, int fileType, String msgId) {
        // 第一步：上传文件
        Map<String, Object> uploadResult;
        if ("group".equals(msgType)) {
            String groupOpenId = (String) attributes.get("groupOpenId");
            uploadResult = apiClient.uploadGroupFile(groupOpenId, fileType, fileUrl, false);
        } else {
            String userOpenId = (String) attributes.get("userOpenId");
            uploadResult = apiClient.uploadC2cFile(userOpenId, fileType, fileUrl, false);
        }

        Object fileInfo = uploadResult.get("file_info");
        if (fileInfo == null) {
            log.error("富媒体上传失败，未返回 file_info: {}", uploadResult);
            return uploadResult;
        }

        // 第二步：发送包含 file_info 的消息
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("msg_type", 7); // 富媒体类型
        body.put("media", Map.of("file_info", fileInfo));
        if (msgId != null && !msgId.isBlank()) {
            body.put("msg_id", msgId);
        } else {
            body.put("msg_seq", msgSeqCounter.getAndIncrement());
        }

        return doSend(msgType, attributes, Map.copyOf(body));
    }

    // ── 发送路由 ────────────────────────────────────────────────

    private Map<String, Object> doSend(String msgType, Map<String, Object> attributes, Map<String, Object> body) {
        return switch (msgType != null ? msgType : "") {
            case "group" -> {
                String groupOpenId = (String) attributes.get("groupOpenId");
                log.info("发送群消息: groupOpenId={}, msg_type={}", groupOpenId, body.get("msg_type"));
                yield apiClient.sendGroupMessage(groupOpenId, body);
            }
            case "c2c" -> {
                String userOpenId = (String) attributes.get("userOpenId");
                log.info("发送私聊消息: userOpenId={}, msg_type={}", userOpenId, body.get("msg_type"));
                yield apiClient.sendC2cMessage(userOpenId, body);
            }
            default -> {
                log.warn("未知的消息类型: {}，跳过发送", msgType);
                yield Map.of("error", "未知消息类型: " + msgType);
            }
        };
    }

    // ── 工具方法 ────────────────────────────────────────────────

    /**
     * 检查是否仍在被动回复 5 分钟窗口内。
     */
    private boolean isWithinPassiveWindow(String receivedAt) {
        if (receivedAt == null || receivedAt.isBlank()) return false;
        try {
            Instant received = Instant.parse(receivedAt);
            return Duration.between(received, Instant.now()).compareTo(PASSIVE_REPLY_WINDOW) < 0;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 将文本按最大长度分段，在换行符或空格处断开。
     */
    static List<String> segmentText(String text, int maxLength) {
        if (text == null || text.isEmpty()) return List.of("");
        if (text.length() <= maxLength) return List.of(text);

        List<String> segments = new ArrayList<>();
        int start = 0;
        while (start < text.length()) {
            int end = Math.min(start + maxLength, text.length());
            if (end < text.length()) {
                // 尝试在换行符处断开
                int breakAt = text.lastIndexOf('\n', end);
                if (breakAt <= start) {
                    // 没有换行符，尝试空格
                    breakAt = text.lastIndexOf(' ', end);
                }
                if (breakAt > start) {
                    end = breakAt + 1;
                }
            }
            segments.add(text.substring(start, end));
            start = end;
        }
        return List.copyOf(segments);
    }
}
