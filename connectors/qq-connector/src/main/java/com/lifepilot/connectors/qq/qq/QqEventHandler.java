package com.lifepilot.connectors.qq.qq;

import com.lifepilot.connectors.qq.model.ConnectorEventRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * QQ 事件转换器，将 QQ 网关推送的事件转换为统一的 {@link ConnectorEventRequest}。
 *
 * <p>支持的事件类型：
 * <ul>
 *   <li>{@code GROUP_AT_MESSAGE_CREATE} — 群 @机器人消息（含图片/文件附件）</li>
 *   <li>{@code C2C_MESSAGE_CREATE} — 私聊消息（含图片/文件附件）</li>
 *   <li>{@code GROUP_MSG_REJECT} / {@code GROUP_MSG_RECEIVE} — 群消息设置变更</li>
 *   <li>{@code FRIEND_ADD} / {@code FRIEND_DEL} — 好友关系变更</li>
 *   <li>{@code GROUP_ADD_ROBOT} / {@code GROUP_DEL_ROBOT} — 机器人入群/退群</li>
 * </ul>
 *
 * <p>内置事件 ID 去重缓存（最近 {@value #DEDUP_CACHE_MAX_SIZE} 条），
 * 防止网络重传导致重复处理。
 *
 * @author zsg
 * @since 2026-04-03
 */
public class QqEventHandler {

    private static final Logger log = LoggerFactory.getLogger(QqEventHandler.class);
    private static final int DEDUP_CACHE_MAX_SIZE = 2048;

    private final String instanceId;
    /** 最近处理过的事件 ID 缓存（LRU，用于去重）。 */
    private final Map<String, Boolean> recentEventIds = Collections.synchronizedMap(
            new LinkedHashMap<>(256, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > DEDUP_CACHE_MAX_SIZE;
                }
            }
    );

    public QqEventHandler(String instanceId) {
        this.instanceId = instanceId;
    }

    /**
     * 将 QQ 分发事件转换为统一入站事件请求。
     *
     * @param event QQ 网关推送的事件
     * @return 转换后的请求，若事件类型不支持或为重复事件则返回 null
     */
    @SuppressWarnings("unchecked")
    public ConnectorEventRequest convert(QqWebSocketManager.QqDispatchEvent event) {
        return switch (event.type()) {
            case "GROUP_AT_MESSAGE_CREATE" -> convertGroupMessage(event.data());
            case "C2C_MESSAGE_CREATE" -> convertC2cMessage(event.data());
            case "GROUP_MSG_REJECT", "GROUP_MSG_RECEIVE",
                 "FRIEND_ADD", "FRIEND_DEL",
                 "GROUP_ADD_ROBOT", "GROUP_DEL_ROBOT" -> convertLifecycleEvent(event.type(), event.data());
            default -> {
                log.debug("忽略不支持的事件类型: {}", event.type());
                yield null;
            }
        };
    }

    // ── 群消息 ──────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private ConnectorEventRequest convertGroupMessage(Map<String, Object> data) {
        String groupOpenId = (String) data.get("group_openid");
        Map<String, Object> author = (Map<String, Object>) data.get("author");
        String memberOpenId = (String) author.get("member_openid");
        String msgId = (String) data.get("id");
        String content = stripBotMention(nullToEmpty((String) data.get("content")));
        String timestamp = (String) data.get("timestamp");

        // 去重
        if (isDuplicate(msgId)) return null;

        // 解析附件
        List<ConnectorEventRequest.Attachment> attachments = extractAttachments(data);

        // 判断内容类型
        String contentType = attachments.isEmpty() ? "text" : "image";

        Map<String, Object> replyAttributes = new LinkedHashMap<>();
        replyAttributes.put("groupOpenId", groupOpenId);
        replyAttributes.put("msgId", msgId);
        replyAttributes.put("msgType", "group");
        replyAttributes.put("receivedAt", Instant.now().toString());

        return new ConnectorEventRequest(
                msgId,
                msgId,
                memberOpenId,
                instanceId + ":group:" + groupOpenId,
                new ConnectorEventRequest.Content(contentType, content, null, null),
                attachments,
                buildMetadata("GROUP_AT_MESSAGE_CREATE", data),
                new ConnectorEventRequest.DeliveryHints("ASYNC_PUSH"),
                new ConnectorEventRequest.ReplyTarget(memberOpenId, instanceId + ":group:" + groupOpenId, Map.copyOf(replyAttributes)),
                parseTimestamp(timestamp)
        );
    }

    // ── 私聊消息 ────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private ConnectorEventRequest convertC2cMessage(Map<String, Object> data) {
        Map<String, Object> author = (Map<String, Object>) data.get("author");
        String userOpenId = (String) author.get("user_openid");
        String msgId = (String) data.get("id");
        String content = nullToEmpty((String) data.get("content")).trim();
        String timestamp = (String) data.get("timestamp");

        // 去重
        if (isDuplicate(msgId)) return null;

        // 解析附件
        List<ConnectorEventRequest.Attachment> attachments = extractAttachments(data);
        String contentType = attachments.isEmpty() ? "text" : "image";

        Map<String, Object> replyAttributes = new LinkedHashMap<>();
        replyAttributes.put("userOpenId", userOpenId);
        replyAttributes.put("msgId", msgId);
        replyAttributes.put("msgType", "c2c");
        replyAttributes.put("receivedAt", Instant.now().toString());

        return new ConnectorEventRequest(
                msgId,
                msgId,
                userOpenId,
                instanceId + ":c2c:" + userOpenId,
                new ConnectorEventRequest.Content(contentType, content, null, null),
                attachments,
                buildMetadata("C2C_MESSAGE_CREATE", data),
                new ConnectorEventRequest.DeliveryHints("ASYNC_PUSH"),
                new ConnectorEventRequest.ReplyTarget(userOpenId, instanceId + ":c2c:" + userOpenId, Map.copyOf(replyAttributes)),
                parseTimestamp(timestamp)
        );
    }

    // ── 生命周期事件 ────────────────────────────────────────────

    private ConnectorEventRequest convertLifecycleEvent(String eventType, Map<String, Object> data) {
        String groupOpenId = (String) data.get("group_openid");
        String userOpenId = (String) data.get("user_openid");
        String eventName = eventType.toLowerCase();

        // 确定 sessionId 和 userId
        String sessionId;
        String userId;
        if (groupOpenId != null) {
            sessionId = instanceId + ":group:" + groupOpenId;
            userId = userOpenId != null ? userOpenId : "system";
        } else if (userOpenId != null) {
            sessionId = instanceId + ":c2c:" + userOpenId;
            userId = userOpenId;
        } else {
            sessionId = instanceId + ":system";
            userId = "system";
        }

        return new ConnectorEventRequest(
                null,
                null,
                userId,
                sessionId,
                new ConnectorEventRequest.Content("event", null, eventName, Map.copyOf(data)),
                List.of(),
                buildMetadata(eventType, data),
                null,
                null,
                Instant.now()
        );
    }

    // ── 附件解析 ────────────────────────────────────────────────

    /**
     * 从 QQ 消息数据中提取附件信息。
     *
     * <p>QQ 消息中的图片等附件在 {@code attachments} 数组中，
     * 每个元素包含 {@code content_type}（MIME）和 {@code url}（下载地址）。
     */
    @SuppressWarnings("unchecked")
    private List<ConnectorEventRequest.Attachment> extractAttachments(Map<String, Object> data) {
        Object raw = data.get("attachments");
        if (!(raw instanceof List<?> rawList) || rawList.isEmpty()) {
            return List.of();
        }
        List<ConnectorEventRequest.Attachment> result = new ArrayList<>();
        for (Object item : rawList) {
            if (item instanceof Map<?, ?> att) {
                String contentType = (String) att.get("content_type");
                String url = (String) att.get("url");
                String filename = (String) att.get("filename");
                if (url != null) {
                    // QQ 的附件以 URL 形式提供，不是 base64；这里将 URL 存入 base64Data 字段
                    // 主服务根据 mimeType 和内容格式判断处理方式
                    result.add(new ConnectorEventRequest.Attachment(
                            null,
                            filename,
                            contentType,
                            url,  // QQ 附件 URL（非 base64，主服务需适配）
                            0
                    ));
                }
            }
        }
        return List.copyOf(result);
    }

    // ── 工具方法 ────────────────────────────────────────────────

    /**
     * 去除群消息中的 @机器人 提及文本。
     *
     * <p>QQ 群消息 content 中：
     * <ul>
     *   <li>在公域机器人下，content 不含 @提及文本，仅有消息正文</li>
     *   <li>在私域/部分场景下，可能包含多余前导空格</li>
     * </ul>
     */
    private String stripBotMention(String content) {
        return content.strip();
    }

    private boolean isDuplicate(String eventId) {
        if (eventId == null || eventId.isBlank()) return false;
        Boolean previous = recentEventIds.putIfAbsent(eventId, Boolean.TRUE);
        if (previous != null) {
            log.debug("跳过重复事件: {}", eventId);
            return true;
        }
        return false;
    }

    private Map<String, Object> buildMetadata(String eventType, Map<String, Object> data) {
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("source", "qq-connector");
        metadata.put("platform", "qq");
        metadata.put("eventType", eventType);
        // 保留原始 group_openid 和 msg_id 以便追溯
        if (data.get("group_openid") != null) metadata.put("groupOpenId", data.get("group_openid"));
        if (data.get("id") != null) metadata.put("originalMsgId", data.get("id"));
        return Map.copyOf(metadata);
    }

    private Instant parseTimestamp(String timestamp) {
        if (timestamp == null || timestamp.isBlank()) return Instant.now();
        try {
            return Instant.parse(timestamp);
        } catch (Exception e) {
            log.debug("时间戳解析失败: {}，使用当前时间", timestamp);
            return Instant.now();
        }
    }

    private String nullToEmpty(String value) {
        return value != null ? value : "";
    }
}
