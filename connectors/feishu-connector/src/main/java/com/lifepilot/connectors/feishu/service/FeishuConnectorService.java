package com.lifepilot.connectors.feishu.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lifepilot.connectors.feishu.config.ConnectorRuntimeProperties;
import com.lifepilot.connectors.feishu.model.ConnectorDeliveryRequest;
import com.lifepilot.connectors.feishu.model.ConnectorEventRequest;
import com.lifepilot.connectors.feishu.model.ConnectorEventResponse;
import com.lifepilot.connectors.feishu.model.ConnectorInstanceCommandRequest;
import com.lifepilot.connectors.feishu.model.ConnectorOperationRequest;
import com.lifepilot.connectors.feishu.model.ConnectorOperationResponse;
import com.lifepilot.connectors.feishu.streaming.FlushOutcome;
import com.lifepilot.connectors.feishu.streaming.StreamingDeliveryQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 飞书 connector 运行时。
 *
 * @author zsg
 * @since 2026-03-29
 */
@Service
public class FeishuConnectorService {

    private static final Logger log = LoggerFactory.getLogger(FeishuConnectorService.class);
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private final ConcurrentHashMap<String, FeishuInstanceState> instances = new ConcurrentHashMap<>();
    private final ConnectorCallbackClient connectorCallbackClient;
    private final FeishuPlatformSessionFactory sessionFactory;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;
    private final long imageMaxBytes;
    private final StreamingDeliveryQueue streamingQueue;

    public FeishuConnectorService(ConnectorCallbackClient connectorCallbackClient,
                                  FeishuPlatformSessionFactory sessionFactory,
                                  ObjectMapper objectMapper,
                                  ConnectorRuntimeProperties runtimeProperties,
                                  StreamingDeliveryQueue streamingQueue) {
        this.connectorCallbackClient = connectorCallbackClient;
        this.sessionFactory = sessionFactory;
        this.objectMapper = objectMapper;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(runtimeProperties.getImageDownloadTimeout())
                .build();
        this.imageMaxBytes = runtimeProperties.getImageMaxBytes();
        this.streamingQueue = streamingQueue;
    }

    public boolean hasInstance(String instanceId) {
        return instances.containsKey(instanceId);
    }

    public boolean matchesToken(String instanceId, String runtimeToken) {
        FeishuInstanceState state = instances.get(instanceId);
        return state != null && state.runtimeToken().equals(runtimeToken);
    }

    public Map<String, Object> start(String instanceId,
                                     String runtimeToken,
                                     ConnectorInstanceCommandRequest request) {
        FeishuInstanceSettings settings = FeishuInstanceSettings.from(request);
        validateConnectionMode(settings.connectionMode());

        FeishuPlatformSession session = sessionFactory.create(settings, inbound -> handleInboundMessage(instanceId, inbound));
        FeishuInstanceState next = new FeishuInstanceState(instanceId, runtimeToken);
        next.start(request, settings, session);

        FeishuInstanceState previous = instances.put(instanceId, next);
        if (previous != null) {
            previous.stopQuietly();
        }
        return next.commandResult("RUNNING", "飞书 connector 已启动");
    }

    public Map<String, Object> stop(String instanceId) {
        FeishuInstanceState state = requireState(instanceId);
        state.stop();
        return state.commandResult("STOPPED", "飞书 connector 已停止");
    }

    public Map<String, Object> reload(String instanceId,
                                      ConnectorInstanceCommandRequest request) {
        FeishuInstanceState state = requireState(instanceId);
        FeishuInstanceSettings settings = FeishuInstanceSettings.from(request);
        validateConnectionMode(settings.connectionMode());

        FeishuPlatformSession session = sessionFactory.create(settings, inbound -> handleInboundMessage(instanceId, inbound));
        state.reload(request, settings, session);
        return state.commandResult("RUNNING", "飞书 connector 已重载");
    }

    public Map<String, Object> health(String instanceId) {
        return requireState(instanceId).healthResult();
    }

    public FeishuWebhookResponse handleWebhook(String instanceId,
                                               FeishuWebhookRequest request) throws Exception {
        FeishuInstanceState state = requireState(instanceId);
        if (!state.running()) {
            throw new IllegalStateException("飞书实例未运行");
        }
        if (!state.webhookMode()) {
            throw new IllegalStateException("当前实例未启用 webhook 模式");
        }
        verifyWebhookToken(instanceId, state, request);
        return state.handleWebhook(request);
    }

    /**
     * 校验 webhook 请求中的 Verification Token。
     * 从请求体 JSON 中提取 token（v1 格式）或 header.token（v2 格式），
     * 与实例配置的 verificationToken 比对，不匹配则抛出安全异常。
     */
    private void verifyWebhookToken(String instanceId, FeishuInstanceState state, FeishuWebhookRequest request) {
        String configuredToken = state.verificationToken();
        if (configuredToken == null || configuredToken.isBlank()) {
            return;
        }
        String requestToken = extractWebhookToken(request);
        if (requestToken == null || requestToken.isBlank()) {
            log.debug("飞书 webhook 请求未携带 token，跳过校验: instanceId={}", instanceId);
            return;
        }
        if (!configuredToken.equals(requestToken)) {
            log.warn("飞书 webhook Verification Token 校验失败: instanceId={}", instanceId);
            throw new SecurityException("Verification Token 校验失败");
        }
        log.debug("飞书 webhook Verification Token 校验通过: instanceId={}", instanceId);
    }

    /**
     * 从 webhook 请求体中提取 token 字段。
     * 支持 v1 格式（顶层 token 字段）和 v2 格式（header.token 字段）。
     */
    @Nullable
    private String extractWebhookToken(FeishuWebhookRequest request) {
        if (request.body() == null || request.body().length == 0) {
            return null;
        }
        try {
            JsonNode root = objectMapper.readTree(request.body());
            if (root == null) {
                return null;
            }
            // v1 格式：顶层 token
            JsonNode tokenNode = root.path("token");
            if (tokenNode.isTextual() && !tokenNode.asText().isBlank()) {
                return tokenNode.asText().trim();
            }
            // v2 格式：header.token
            JsonNode headerToken = root.path("header").path("token");
            if (headerToken.isTextual() && !headerToken.asText().isBlank()) {
                return headerToken.asText().trim();
            }
            return null;
        } catch (Exception ex) {
            log.debug("解析 webhook 请求体提取 token 失败: {}", ex.getMessage());
            return null;
        }
    }

    public Map<String, Object> deliver(String instanceId,
                                       ConnectorDeliveryRequest request) throws Exception {
        FeishuInstanceState state = requireState(instanceId);
        FeishuOutgoingMessage outgoingMessage = toOutgoingMessage(request, state);
        FeishuSendResult result = state.send(outgoingMessage);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("accepted", true);
        payload.put("instanceId", instanceId);
        payload.put("responseId", request.responseId());
        payload.put("deliveryCount", state.deliveryCount());
        payload.put("deliveredAt", state.lastDeliveredAt());
        payload.put("contentType", outgoingMessage.msgType());
        payload.put("route", result.route());
        if (result.messageId() != null && !result.messageId().isBlank()) {
            payload.put("messageId", result.messageId());
        }
        return Map.copyOf(payload);
    }

    /**
     * 接收主服务流式 deliver 请求，把最新一份 accText 入队等待 scheduler 异步 flush。
     *
     * <p>此方法立刻返回，不直接调飞书 SDK，避免高频 PATCH 流量拖垮接口。真正落地由
     * {@link com.lifepilot.connectors.feishu.streaming.StreamingDeliveryFlushScheduler}
     * 按节流策略调用 {@link #flushStreamingDelivery}。</p>
     *
     * @param instanceId 飞书实例 id
     * @param request    含 responseId / content 的请求；不存在的实例会抛出 IllegalArgumentException
     */
    public void enqueueStreamingDelivery(String instanceId, ConnectorDeliveryRequest request) {
        // 校验实例存在；未启动直接 fail-fast，避免 lost update
        requireState(instanceId);
        String responseId = request.responseId();
        if (responseId == null || responseId.isBlank()) {
            throw new IllegalArgumentException("流式投递缺少 responseId");
        }
        String accText = extractStreamingText(request);
        streamingQueue.enqueue(instanceId, responseId, accText, Instant.now());
    }

    /**
     * 真实调用飞书 SDK 落地一次流式 flush — 已发过则 update，否则 send 后保存映射。
     *
     * <p>异常归一化策略（{@code IllegalStateException} 消息中通常含 {@code code=<num>} 模式）：
     * <ul>
     *   <li>消息含 429 / "rate" 关键字 → {@link FlushOutcome#RATE_LIMITED}</li>
     *   <li>消息含 404 / 410 / 230002（飞书消息撤回）关键字 → 删除 SentMessageRef +
     *       {@link FlushOutcome#NOT_FOUND}（下次 tick 同 rid 会走 send 路径）</li>
     *   <li>消息含 5xx 关键字 → {@link FlushOutcome#TRANSIENT}</li>
     *   <li>未识别异常 → {@link FlushOutcome#TRANSIENT}（保守归类，让 scheduler 重试）</li>
     * </ul>
     *
     * <p>TODO(T16 端到端冒烟): 字符串匹配是临时实现，端到端跑通后改为按 SDK 异常类型 / code 精准映射。</p>
     *
     * @param instanceId 实例 id
     * @param responseId 主服务侧 stable id
     * @param accText    截至当前的累积文本快照
     * @return outcome — scheduler 据此决定退避 / 重试
     */
    public FlushOutcome flushStreamingDelivery(String instanceId, String responseId, String accText) {
        FeishuInstanceState state = requireState(instanceId);
        FeishuSentMessageRef sentRef = state.findResponseMessage(responseId);

        try {
            if (sentRef != null) {
                // 已发过 → 调 update；msgType 沿用首次发送时的类型，避免 text↔post 混发被飞书拒绝
                state.updateMessage(sentRef.messageId(), sentRef.msgType(), serializeTextContent(accText));
                return FlushOutcome.OK;
            }
            // 首次 → 走 text 路径 send；state.send 会按 message.uuid()=responseId 自动保存 SentMessageRef
            FeishuOutgoingMessage outgoing = buildStreamingTextMessage(state, responseId, accText);
            state.send(outgoing);
            return FlushOutcome.OK;
        } catch (Exception ex) {
            return mapToOutcome(ex, instanceId, responseId, state);
        }
    }

    /**
     * 构建流式首次发送用的 text 消息 — 复用 lastChatId 作为 receiveId（同一 turn 应已存在）。
     *
     * <p>流式 PATCH 的语义是"主服务同一 turn 内多次推增量"，首次入队时应已经有过一次
     * 同 chat 的入站事件，state.lastChatId 即为合理的目标。若取不到则抛错，让 scheduler 归 TRANSIENT 重试。</p>
     */
    private FeishuOutgoingMessage buildStreamingTextMessage(FeishuInstanceState state,
                                                             String responseId,
                                                             String accText) {
        String chatId = state.lastChatId();
        if (chatId == null || chatId.isBlank()) {
            throw new IllegalStateException("流式投递无可用 chatId（实例尚未收到任何入站消息）");
        }
        return new FeishuOutgoingMessage(
                FeishuSendRoute.CREATE,
                null,
                chatId,
                "chat_id",
                "text",
                serializeTextContent(accText),
                false,
                responseId,
                accText
        );
    }

    /**
     * 从 deliver 请求中抽取流式文本快照 — 优先取 content.plainText，缺省则空串。
     */
    private String extractStreamingText(ConnectorDeliveryRequest request) {
        ConnectorDeliveryRequest.Content content = request.content();
        if (content == null) {
            return "";
        }
        return defaultString(content.plainText(), "");
    }

    /**
     * 把 SDK 异常映射为 {@link FlushOutcome}；未识别归 TRANSIENT 让 scheduler 下次重试。
     */
    private FlushOutcome mapToOutcome(Exception ex, String instanceId, String responseId,
                                      FeishuInstanceState state) {
        String msg = ex.getMessage() != null ? ex.getMessage() : "";
        String lower = msg.toLowerCase();
        if (msg.contains("429") || lower.contains("rate") || msg.contains("99991663")) {
            log.debug("飞书 flush 触发 429 退避: instanceId={}, rid={}, error={}",
                    instanceId, responseId, msg);
            return FlushOutcome.RATE_LIMITED;
        }
        // 飞书消息撤回错误码 230002 / 通用 404 / 410 都视为消息不存在 → 删 ref 让下次走 send 路径
        if (msg.contains("404") || msg.contains("410") || msg.contains("230002")) {
            state.removeResponseMessage(responseId);
            log.info("飞书 flush 命中消息不存在: instanceId={}, rid={}, error={}",
                    instanceId, responseId, msg);
            return FlushOutcome.NOT_FOUND;
        }
        if (msg.contains("500") || msg.contains("502") || msg.contains("503") || msg.contains("504")) {
            log.debug("飞书 flush 5xx 临时故障: instanceId={}, rid={}, error={}",
                    instanceId, responseId, msg);
            return FlushOutcome.TRANSIENT;
        }
        log.warn("飞书 flush 未识别异常，归 TRANSIENT 重试: instanceId={}, rid={}, error={}",
                instanceId, responseId, msg);
        return FlushOutcome.TRANSIENT;
    }

    /**
     * 处理操作请求，按 operationType 路由到具体飞书 API 调用。
     */
    public ConnectorOperationResponse handleOperation(String instanceId,
                                                       ConnectorOperationRequest request) {
        FeishuInstanceState state = requireState(instanceId);
        if (!state.running()) {
            return ConnectorOperationResponse.failure(request.operationId(), "飞书实例未运行");
        }
        String operationType = request.operationType() != null ? request.operationType().trim() : "";
        log.info("飞书操作请求: instanceId={}, operationId={}, operationType={}", instanceId, request.operationId(), operationType);
        try {
            Map<String, Object> result = switch (operationType) {
                case "message_update" -> handleMessageUpdate(state, request.parameters());
                case "message_recall" -> handleMessageRecall(state, request.parameters());
                case "file_upload" -> handleFileUpload(state, request.parameters());
                case "file_download" -> handleFileDownload(state, request.parameters());
                case "group_create" -> handleGroupCreate(state, request.parameters());
                case "group_member_manage" -> handleGroupMemberManage(state, request.parameters());
                case "task_create" -> handleTaskCreate(state, request.parameters());
                case "document_create" -> handleDocumentCreate(state, request.parameters());
                case "calendar_event_create" -> handleCalendarEventCreate(state, request.parameters());
                default -> {
                    log.warn("未知操作类型: instanceId={}, operationType={}", instanceId, operationType);
                    yield null;
                }
            };
            if (result == null) {
                return ConnectorOperationResponse.failure(request.operationId(), "不支持的操作类型: " + operationType);
            }
            return ConnectorOperationResponse.success(request.operationId(), result);
        } catch (Exception ex) {
            log.warn("飞书操作执行失败: instanceId={}, operationId={}, operationType={}", instanceId, request.operationId(), operationType, ex);
            return ConnectorOperationResponse.failure(request.operationId(), ex.getMessage());
        }
    }

    private Map<String, Object> handleMessageUpdate(FeishuInstanceState state,
                                                     Map<String, Object> parameters) throws Exception {
        String messageId = requireParam(parameters, "messageId");
        String msgType = defaultString(asString(parameters.get("msgType")), "text");
        String content = requireParam(parameters, "content");
        return state.updateMessage(messageId, msgType, content);
    }

    private Map<String, Object> handleMessageRecall(FeishuInstanceState state,
                                                     Map<String, Object> parameters) throws Exception {
        String messageId = requireParam(parameters, "messageId");
        return state.recallMessage(messageId);
    }

    private Map<String, Object> handleFileUpload(FeishuInstanceState state,
                                                  Map<String, Object> parameters) throws Exception {
        String fileType = defaultString(asString(parameters.get("fileType")), "image");
        String base64Data = requireParam(parameters, "base64Data");
        byte[] data = Base64.getDecoder().decode(base64Data);
        verifyUploadSize(data.length, fileType);
        if ("image".equalsIgnoreCase(fileType)) {
            String imageKey = state.uploadImage(new FeishuImageUpload(
                    asString(parameters.get("fileName")),
                    asString(parameters.get("mimeType")),
                    data
            ));
            return Map.of("imageKey", imageKey, "fileType", "image");
        }
        String fileKey = state.uploadFile(new FeishuFileUpload(
                asString(parameters.get("fileName")),
                asString(parameters.get("mimeType")),
                data
        ));
        return Map.of("fileKey", fileKey, "fileType", fileType);
    }

    private Map<String, Object> handleFileDownload(FeishuInstanceState state,
                                                    Map<String, Object> parameters) throws Exception {
        String messageId = requireParam(parameters, "messageId");
        String fileKey = requireParam(parameters, "fileKey");
        String type = defaultString(asString(parameters.get("type")), "file");
        return state.downloadResource(messageId, fileKey, type);
    }

    private Map<String, Object> handleGroupCreate(FeishuInstanceState state,
                                                   Map<String, Object> parameters) throws Exception {
        String name = requireParam(parameters, "name");
        String description = asString(parameters.get("description"));
        String[] userIdList = extractStringArray(parameters.get("userIdList"));
        return state.createChat(name, description, userIdList);
    }

    private Map<String, Object> handleGroupMemberManage(FeishuInstanceState state,
                                                         Map<String, Object> parameters) throws Exception {
        String chatId = requireParam(parameters, "chatId");
        String action = defaultString(asString(parameters.get("action")), "add");
        String memberIdType = defaultString(asString(parameters.get("memberIdType")), "open_id");
        String[] idList = extractStringArray(parameters.get("idList"));
        if (idList == null || idList.length == 0) {
            throw new IllegalArgumentException("群成员管理缺少 idList");
        }
        return switch (action.toLowerCase()) {
            case "add" -> state.addChatMembers(chatId, memberIdType, idList);
            case "remove" -> state.removeChatMembers(chatId, memberIdType, idList);
            default -> throw new IllegalArgumentException("不支持的群成员操作: " + action);
        };
    }

    /**
     * 处理任务创建操作。
     *
     * @author zsg
     * @since 2026-04-02
     */
    private Map<String, Object> handleTaskCreate(FeishuInstanceState state,
                                                  Map<String, Object> parameters) throws Exception {
        String summary = requireParam(parameters, "summary");
        String dueTimestamp = asString(parameters.get("dueTimestamp"));
        return state.createTask(summary, dueTimestamp);
    }

    /**
     * 处理文档创建操作。
     *
     * @author zsg
     * @since 2026-04-02
     */
    private Map<String, Object> handleDocumentCreate(FeishuInstanceState state,
                                                      Map<String, Object> parameters) throws Exception {
        String title = requireParam(parameters, "title");
        String folderToken = asString(parameters.get("folderToken"));
        return state.createDocument(title, folderToken);
    }

    /**
     * 处理日程事件创建操作。
     *
     * @author zsg
     * @since 2026-04-02
     */
    private Map<String, Object> handleCalendarEventCreate(FeishuInstanceState state,
                                                           Map<String, Object> parameters) throws Exception {
        String calendarId = requireParam(parameters, "calendarId");
        String summary = requireParam(parameters, "summary");
        String startTime = requireParam(parameters, "startTime");
        String endTime = requireParam(parameters, "endTime");
        return state.createCalendarEvent(calendarId, summary, startTime, endTime);
    }

    private String requireParam(Map<String, Object> parameters, String key) {
        String value = asString(parameters.get(key));
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("操作缺少必要参数: " + key);
        }
        return value;
    }

    @Nullable
    private String[] extractStringArray(@Nullable Object value) {
        if (value instanceof List<?> list) {
            return list.stream()
                    .filter(item -> item instanceof String)
                    .map(Object::toString)
                    .toArray(String[]::new);
        }
        if (value instanceof String[] arr) {
            return arr;
        }
        return null;
    }

    public Map<String, Object> forwardHeartbeat(String instanceId) {
        FeishuInstanceState state = requireState(instanceId);
        Map<String, Object> response = connectorCallbackClient.forwardHeartbeat(instanceId, state.runtimeToken());
        state.recordHeartbeat();
        return response != null ? response : Map.of();
    }

    FeishuInboundHandlingResult handleInboundMessage(String instanceId, FeishuInboundMessage message) {
        FeishuInstanceState state = requireState(instanceId);
        if (!state.running()) {
            state.recordIgnoredEvent("实例未运行");
            return FeishuInboundHandlingResult.ignored("warning", "实例未运行");
        }
        if (message.senderType() != null && !"user".equalsIgnoreCase(message.senderType())) {
            state.recordIgnoredEvent("忽略非用户消息");
            log.debug("忽略飞书非用户消息: instanceId={}, senderType={}", instanceId, message.senderType());
            return FeishuInboundHandlingResult.ignored("info", "已忽略非用户消息");
        }

        ConnectorEventRequest request = toConnectorEventRequest(state, message);
        ConnectorEventResponse response;
        try {
            response = connectorCallbackClient.submitEvent(instanceId, state.runtimeToken(), request);
        } catch (RuntimeException ex) {
            state.recordCallbackFailure(ex);
            log.warn("飞书入站事件回调知微失败: instanceId={}, messageId={}", instanceId, message.messageId(), ex);
            return FeishuInboundHandlingResult.ignored("warning", "操作提交失败，请稍后重试");
        }

        state.recordInboundEvent(message, response.responseId());
        if (!response.accepted()) {
            state.recordCallbackRejection(response.errorMessage());
            return FeishuInboundHandlingResult.ignored("warning", firstNonBlank(response.errorMessage(), "当前操作未被受理"));
        }

        for (Map<String, Object> rawDelivery : response.deliveries()) {
            try {
                ConnectorDeliveryRequest deliveryRequest = objectMapper.convertValue(rawDelivery, ConnectorDeliveryRequest.class);
                deliver(instanceId, deliveryRequest);
            } catch (Exception ex) {
                state.recordDeliveryFailure(ex);
                log.warn("飞书回调结果投递失败: instanceId={}, responseId={}", instanceId, response.responseId(), ex);
            }
        }
        return FeishuInboundHandlingResult.accepted(
                "info",
                resolveInboundToast(message, response)
        );
    }

    private ConnectorEventRequest toConnectorEventRequest(FeishuInstanceState state,
                                                          FeishuInboundMessage message) {
        JsonNode contentJson = parseJson(message.content());
        if ("card-action".equalsIgnoreCase(message.messageType())) {
            return toCardActionEventRequest(state, message, contentJson);
        }

        String msgType = message.messageType() != null ? message.messageType().trim().toLowerCase() : "text";
        String contentType = resolveInboundContentType(msgType);
        String text = extractText(message.messageType(), contentJson, message.content());
        Map<String, Object> payload = buildInboundPayload(msgType, contentJson, message);
        Map<String, Object> metadata = mergeMetadata(state, message);
        List<ConnectorEventRequest.Attachment> attachments = buildInboundAttachments(msgType, contentJson, state, message);

        return new ConnectorEventRequest(
                message.eventId(),
                message.messageId(),
                resolveUserId(message),
                message.chatId(),
                new ConnectorEventRequest.Content(contentType, text, null, payload),
                attachments,
                metadata,
                new ConnectorEventRequest.DeliveryHints("ASYNC_PUSH"),
                new ConnectorEventRequest.ReplyTarget(
                        resolveUserId(message),
                        message.chatId(),
                        buildReplyAttributes(message)
                ),
                message.occurredAt() != null ? message.occurredAt() : Instant.now()
        );
    }

    /**
     * 将飞书 msg_type 映射为统一的 content.type。
     */
    private String resolveInboundContentType(String msgType) {
        return switch (msgType) {
            case "image" -> "image";
            case "file" -> "file";
            case "audio" -> "audio";
            case "media" -> "video";
            case "post" -> "text";
            default -> msgType;
        };
    }

    /**
     * 根据消息类型构建入站 payload，包含文件元数据（fileToken、fileName 等）。
     */
    private Map<String, Object> buildInboundPayload(String msgType,
                                                     @Nullable JsonNode contentJson,
                                                     FeishuInboundMessage message) {
        Map<String, Object> basePayload = extractPayload(contentJson, message.content());
        if (contentJson == null) {
            return basePayload;
        }
        return switch (msgType) {
            case "image" -> {
                LinkedHashMap<String, Object> payload = new LinkedHashMap<>(basePayload);
                String imageKey = readTextField(contentJson, "image_key");
                if (imageKey != null) {
                    payload.put("fileToken", imageKey);
                }
                payload.put("mimeType", "image/png");
                yield Map.copyOf(payload);
            }
            case "file" -> {
                LinkedHashMap<String, Object> payload = new LinkedHashMap<>(basePayload);
                String fileKey = readTextField(contentJson, "file_key");
                String fileName = readTextField(contentJson, "file_name");
                if (fileKey != null) {
                    payload.put("fileToken", fileKey);
                }
                if (fileName != null) {
                    payload.put("fileName", fileName);
                }
                yield Map.copyOf(payload);
            }
            case "audio" -> {
                LinkedHashMap<String, Object> payload = new LinkedHashMap<>(basePayload);
                String fileKey = readTextField(contentJson, "file_key");
                if (fileKey != null) {
                    payload.put("fileToken", fileKey);
                }
                payload.put("mimeType", "audio/ogg");
                yield Map.copyOf(payload);
            }
            case "media" -> {
                LinkedHashMap<String, Object> payload = new LinkedHashMap<>(basePayload);
                String fileKey = readTextField(contentJson, "file_key");
                String fileName = readTextField(contentJson, "file_name");
                if (fileKey != null) {
                    payload.put("fileToken", fileKey);
                }
                if (fileName != null) {
                    payload.put("fileName", fileName);
                }
                payload.put("mimeType", "video/mp4");
                yield Map.copyOf(payload);
            }
            default -> basePayload;
        };
    }

    /**
     * 对富媒体消息（图片/文件/音频/视频），尝试从飞书下载数据并以 Base64 附件形式返回。
     */
    private List<ConnectorEventRequest.Attachment> buildInboundAttachments(String msgType,
                                                                           @Nullable JsonNode contentJson,
                                                                           FeishuInstanceState state,
                                                                           FeishuInboundMessage message) {
        if (contentJson == null) {
            return List.of();
        }
        try {
            return switch (msgType) {
                case "image" -> {
                    String imageKey = readTextField(contentJson, "image_key");
                    if (imageKey == null || imageKey.isBlank() || message.messageId() == null) {
                        yield List.of();
                    }
                    Map<String, Object> downloaded = state.downloadResource(message.messageId(), imageKey, "image");
                    String base64 = asString(downloaded.get("base64Data"));
                    if (base64 == null) {
                        yield List.of();
                    }
                    Object sizeObj = downloaded.get("size");
                    long size = sizeObj instanceof Number num ? num.longValue() : 0;
                    yield List.of(new ConnectorEventRequest.Attachment(
                            imageKey,
                            asString(downloaded.get("fileName")),
                            "image/png",
                            base64,
                            size
                    ));
                }
                case "file", "audio", "media" -> {
                    String fileKey = readTextField(contentJson, "file_key");
                    if (fileKey == null || fileKey.isBlank() || message.messageId() == null) {
                        yield List.of();
                    }
                    String type = "file".equals(msgType) ? "file" : msgType;
                    Map<String, Object> downloaded = state.downloadResource(message.messageId(), fileKey, type);
                    String base64 = asString(downloaded.get("base64Data"));
                    if (base64 == null) {
                        yield List.of();
                    }
                    Object sizeObj = downloaded.get("size");
                    long size = sizeObj instanceof Number num ? num.longValue() : 0;
                    String fileName = firstNonBlank(readTextField(contentJson, "file_name"), asString(downloaded.get("fileName")));
                    // 飞书 msgType=file 的消息没有 MIME 字段；按 fileName 扩展名推断真实 MIME，
                    // 让主服务能正确把 docx/xlsx/pdf 识别成文档类（否则主服务拿到 octet-stream
                    // 无法走文档 parser 路由、无法做附件上下文注入）
                    String mimeType = inferInboundMimeType(msgType, fileName);
                    yield List.of(new ConnectorEventRequest.Attachment(
                            fileKey,
                            fileName,
                            mimeType,
                            base64,
                            size
                    ));
                }
                default -> List.of();
            };
        } catch (Exception ex) {
            log.warn("飞书富媒体消息下载失败，跳过附件: instanceId={}, messageId={}, msgType={}", state.instanceId(), message.messageId(), msgType, ex);
            return List.of();
        }
    }

    private String resolveInboundToast(FeishuInboundMessage message,
                                       ConnectorEventResponse response) {
        if (!"card-action".equalsIgnoreCase(message.messageType())) {
            return "已收到消息，正在处理中";
        }
        String deliveryText = extractFirstDeliveryText(response.deliveries());
        if (deliveryText != null) {
            return truncateToast(deliveryText);
        }
        JsonNode payload = parseJson(message.content());
        if (payload != null) {
            String actionText = buildCardActionText(extractPayload(payload, message.content()));
            if (actionText != null && !actionText.isBlank()) {
                return truncateToast(actionText);
            }
        }
        return "已收到操作，正在处理中";
    }

    private ConnectorEventRequest toCardActionEventRequest(FeishuInstanceState state,
                                                           FeishuInboundMessage message,
                                                           @Nullable JsonNode contentJson) {
        Map<String, Object> payload = extractPayload(contentJson, message.content());
        String text = buildCardActionText(payload);
        String actionName = resolveCardActionName(payload);
        LinkedHashMap<String, Object> metadata = new LinkedHashMap<>(mergeMetadata(state, message));
        metadata.put("eventKind", "card-action");
        return new ConnectorEventRequest(
                message.eventId(),
                message.messageId(),
                resolveUserId(message),
                message.chatId(),
                new ConnectorEventRequest.Content("card-action", text, actionName, payload),
                List.of(),
                Map.copyOf(metadata),
                new ConnectorEventRequest.DeliveryHints("ASYNC_PUSH"),
                new ConnectorEventRequest.ReplyTarget(
                        resolveUserId(message),
                        message.chatId(),
                        buildReplyAttributes(message)
                ),
                message.occurredAt() != null ? message.occurredAt() : Instant.now()
        );
    }

    private FeishuOutgoingMessage toOutgoingMessage(ConnectorDeliveryRequest request,
                                                    FeishuInstanceState state) throws Exception {
        ConnectorDeliveryRequest.Target target = request.target();
        Map<String, Object> attributes = target != null && target.attributes() != null ? target.attributes() : Map.of();
        FeishuRenderedMessage rendered = renderMessage(request.content(), request.attachments(), state);
        // 消息回复支持：优先使用 attributes.messageId，其次使用 metadata.parentMessageId
        Map<String, Object> metadata = request.metadata() != null ? request.metadata() : Map.of();
        String replyMessageId = firstNonBlank(
                asString(attributes.get("messageId")),
                asString(metadata.get("parentMessageId"))
        );
        boolean replyInThread = asBoolean(attributes.get("replyInThread"))
                || asBoolean(metadata.get("replyInThread"))
                || (asString(attributes.get("threadId")) != null && !asString(attributes.get("threadId")).isBlank());
        FeishuSentMessageRef previousMessage = state.findResponseMessage(request.responseId());

        if (previousMessage != null) {
            if ("interactive".equalsIgnoreCase(previousMessage.msgType())) {
                FeishuRenderedMessage cardRendered = renderInteractiveCompatibleMessage(request.content(), rendered.plainText());
                return new FeishuOutgoingMessage(
                        FeishuSendRoute.PATCH,
                        previousMessage.messageId(),
                        null,
                        "chat_id",
                        "interactive",
                        cardRendered.content(),
                        false,
                        request.responseId(),
                        cardRendered.plainText()
                );
            }
            if (supportsTextOrPostUpdate(previousMessage.msgType()) && supportsTextOrPostUpdate(rendered.msgType())) {
                return new FeishuOutgoingMessage(
                        FeishuSendRoute.UPDATE,
                        previousMessage.messageId(),
                        null,
                        "chat_id",
                        rendered.msgType(),
                        rendered.content(),
                        false,
                        request.responseId(),
                        rendered.plainText()
                );
            }
        }

        if (replyMessageId != null && !replyMessageId.isBlank()) {
            return new FeishuOutgoingMessage(
                    FeishuSendRoute.REPLY,
                    replyMessageId,
                    null,
                    "chat_id",
                    rendered.msgType(),
                    rendered.content(),
                    replyInThread,
                    request.responseId(),
                    rendered.plainText()
            );
        }

        // Phase 6: 多目标类型 — 优先使用 receiveIdType 属性，默认 chat_id 向后兼容
        String explicitReceiveIdType = asString(attributes.get("receiveIdType"));
        String receiveId;
        String receiveIdType;
        if (target != null && target.sessionId() != null && !target.sessionId().isBlank()) {
            receiveId = target.sessionId().trim();
            receiveIdType = defaultString(explicitReceiveIdType, "chat_id");
        } else {
            receiveId = target != null ? target.userId() : null;
            receiveIdType = defaultString(explicitReceiveIdType, "chat_id");
        }

        if (receiveId == null || receiveId.isBlank()) {
            throw new IllegalArgumentException("缺少飞书投递目标");
        }

        return new FeishuOutgoingMessage(
                FeishuSendRoute.CREATE,
                null,
                receiveId,
                receiveIdType,
                rendered.msgType(),
                rendered.content(),
                false,
                request.responseId(),
                rendered.plainText()
        );
    }

    private FeishuRenderedMessage renderMessage(@Nullable ConnectorDeliveryRequest.Content content,
                                                List<ConnectorDeliveryRequest.Attachment> attachments,
                                                FeishuInstanceState state) throws Exception {
        if (content == null) {
            return new FeishuRenderedMessage("text", serializeTextContent(""), "");
        }
        String type = content.type() != null ? content.type().trim().toLowerCase() : "text";
        return switch (type) {
            case "card" -> renderCardMessage(content);
            case "streaming" -> renderInteractiveCompatibleMessage(content, renderStreamingText(content));
            case "markdown" -> renderInteractiveCompatibleMessage(content, defaultString(content.plainText(), ""));
            case "image" -> renderImageMessage(content, attachments, state);
            case "file" -> renderFileMessage(content, attachments, state);
            case "text" -> new FeishuRenderedMessage(
                    "text",
                    serializeTextContent(defaultString(content.plainText(), "")),
                    defaultString(content.plainText(), "")
            );
            default -> new FeishuRenderedMessage(
                    "text",
                    serializeTextContent(defaultString(content.plainText(), "")),
                    defaultString(content.plainText(), "")
            );
        };
    }

    private FeishuRenderedMessage renderImageMessage(ConnectorDeliveryRequest.Content content,
                                                     List<ConnectorDeliveryRequest.Attachment> attachments,
                                                     FeishuInstanceState state) throws Exception {
        String plainText = renderImageText(content);
        try {
            String imageKey = resolveImageKey(content, attachments, state);
            return new FeishuRenderedMessage(
                    "image",
                    writeJson(Map.of("image_key", imageKey)),
                    plainText
            );
        } catch (Exception ex) {
            log.warn("飞书图片上传失败，降级为卡片消息: instanceId={}, error={}", state.instanceId(), ex.getMessage());
            return renderInteractiveCompatibleMessage(content, plainText);
        }
    }

    private FeishuRenderedMessage renderFileMessage(ConnectorDeliveryRequest.Content content,
                                                    List<ConnectorDeliveryRequest.Attachment> attachments,
                                                    FeishuInstanceState state) throws Exception {
        String plainText = renderFileText(content, attachments);
        try {
            String fileKey = resolveFileKey(content, attachments, state);
            return new FeishuRenderedMessage(
                    "file",
                    writeJson(Map.of("file_key", fileKey)),
                    plainText
            );
        } catch (Exception ex) {
            log.warn("飞书文件上传失败，降级为卡片消息: instanceId={}, error={}", state.instanceId(), ex.getMessage());
            return renderInteractiveCompatibleMessage(content, plainText);
        }
    }

    /**
     * 渲染卡片消息：如果 payload 中包含 cardJson，直接使用原始飞书卡片 JSON；否则降级为构建卡片。
     */
    private FeishuRenderedMessage renderCardMessage(ConnectorDeliveryRequest.Content content) {
        Map<String, Object> payload = content.payload() != null ? content.payload() : Map.of();
        Object cardJsonObj = payload.get("cardJson");
        if (cardJsonObj != null) {
            String cardJson;
            if (cardJsonObj instanceof String str) {
                cardJson = str;
            } else {
                cardJson = writeJson(cardJsonObj);
            }
            String plainText = renderCardText(content);
            return new FeishuRenderedMessage("interactive", cardJson, plainText);
        }
        return renderInteractiveCompatibleMessage(content, renderCardText(content));
    }

    private FeishuRenderedMessage renderPostMessage(ConnectorDeliveryRequest.Content content) {
        Map<String, Object> payload = content.payload() != null ? content.payload() : Map.of();
        String markdown = defaultString(asString(payload.get("markdown")), content.plainText());
        Map<String, Object> post = Map.of(
                "zh_cn", Map.of(
                        "content", List.of(List.of(Map.of(
                                "tag", "md",
                                "text", defaultString(markdown, "")
                        )))
                )
        );
        return new FeishuRenderedMessage("post", writeJson(post), defaultString(markdown, ""));
    }

    private FeishuRenderedMessage renderInteractiveCompatibleMessage(ConnectorDeliveryRequest.Content content,
                                                                     String plainText) {
        Map<String, Object> payload = content.payload() != null ? content.payload() : Map.of();
        String title = resolveCardTitle(content, payload);
        List<Map<String, Object>> elements = new ArrayList<>();

        String body = resolveCardBody(content, payload, plainText);
        if (body != null && !body.isBlank()) {
            elements.add(Map.of(
                    "tag", "markdown",
                    "content", body
            ));
        }

        Object actions = payload.get("actions");
        if (actions instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof Map<?, ?> action) {
                    Map<String, Object> actionElement = buildCardActionElement(action);
                    if (actionElement != null) {
                        elements.add(actionElement);
                    }
                }
            }
        }

        Map<String, Object> card = new LinkedHashMap<>();
        card.put("schema", "2.0");
        card.put("config", Map.of("update_multi", true));
        card.put("header", Map.of(
                "title", Map.of(
                        "tag", "plain_text",
                        "content", title
                ),
                "template", "blue"
        ));
        card.put("body", Map.of(
                "direction", "vertical",
                "elements", List.copyOf(elements)
        ));
        return new FeishuRenderedMessage("interactive", writeJson(card), plainText);
    }

    @Nullable
    private Map<String, Object> buildCardActionElement(Map<?, ?> action) {
        String label = asString(action.get("label"));
        if (label == null) {
            return null;
        }
        LinkedHashMap<String, Object> element = new LinkedHashMap<>();
        element.put("tag", "button");
        element.put("text", Map.of(
                "tag", "plain_text",
                "content", label
        ));

        String actionType = defaultString(asString(action.get("type")), "callback");

        if ("url".equals(actionType)) {
            element.put("type", "default");
            element.put("behaviors", List.of(Map.of(
                    "type", "open_url",
                    "default_url", asString(action.get("value"))
            )));
            return Map.copyOf(element);
        }

        // 回调按钮 — schema 2.0 使用 behaviors callback，点击后触发平台回调事件
        element.put("type", "default");
        String callbackValue = defaultString(asString(action.get("value")), label);
        element.put("behaviors", List.of(Map.of(
                "type", "callback",
                "value", Map.of("action", callbackValue)
        )));
        return Map.copyOf(element);
    }

    private String resolveCardTitle(ConnectorDeliveryRequest.Content content,
                                    Map<String, Object> payload) {
        String explicitTitle = asString(payload.get("title"));
        if (explicitTitle != null) {
            return explicitTitle;
        }
        String type = content.type() != null ? content.type().trim().toLowerCase() : "text";
        return switch (type) {
            case "streaming" -> "知微正在生成";
            case "image" -> "图片消息";
            case "file" -> "文件消息";
            default -> "知微消息";
        };
    }

    private String resolveCardBody(ConnectorDeliveryRequest.Content content,
                                   Map<String, Object> payload,
                                   String plainText) {
        String type = content.type() != null ? content.type().trim().toLowerCase() : "text";
        return switch (type) {
            case "card" -> renderCardMarkdownBody(payload, plainText);
            case "streaming" -> renderStreamingMarkdownBody(payload, plainText);
            case "image" -> renderImageMarkdownBody(payload, plainText);
            case "file" -> renderFileMarkdownBody(payload, plainText);
            default -> plainText;
        };
    }

    private String renderCardText(ConnectorDeliveryRequest.Content content) {
        Map<String, Object> payload = content.payload() != null ? content.payload() : Map.of();
        List<String> parts = new ArrayList<>();
        String title = asString(payload.get("title"));
        String body = asString(payload.get("body"));
        if (title != null && !title.isBlank()) {
            parts.add(title.trim());
        }
        if (body != null && !body.isBlank()) {
            parts.add(body.trim());
        }
        Object actions = payload.get("actions");
        if (actions instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof Map<?, ?> action) {
                    String label = asString(action.get("label"));
                    String url = asString(action.get("url"));
                    if (label != null && url != null) {
                        parts.add(label + ": " + url);
                    } else if (label != null) {
                        parts.add("操作: " + label);
                    }
                }
            }
        }
        if (!parts.isEmpty()) {
            return String.join("\n", parts);
        }
        return defaultString(content.plainText(), "");
    }

    private String renderCardMarkdownBody(Map<String, Object> payload,
                                          String plainText) {
        List<String> parts = new ArrayList<>();
        String body = asString(payload.get("body"));
        if (body != null && !body.isBlank()) {
            parts.add(body);
        } else if (plainText != null && !plainText.isBlank()) {
            parts.add(plainText);
        }
        Object actions = payload.get("actions");
        if (actions instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof Map<?, ?> action) {
                    String label = asString(action.get("label"));
                    String url = asString(action.get("url"));
                    if (label != null && url != null) {
                        parts.add("[" + label + "](" + url + ")");
                    } else if (label != null) {
                        parts.add("操作: `" + label + "`");
                    }
                }
            }
        }
        return String.join("\n\n", parts);
    }

    private String renderStreamingText(ConnectorDeliveryRequest.Content content) {
        Map<String, Object> payload = content.payload() != null ? content.payload() : Map.of();
        String streamId = asString(payload.get("streamId"));
        if (content.plainText() != null && !content.plainText().isBlank()
                && !content.plainText().startsWith("[流式响应:")) {
            return content.plainText();
        }
        return streamId != null ? "知微正在生成内容。\n\nstreamId: `" + streamId + "`" : "知微正在生成内容。";
    }

    private String renderStreamingMarkdownBody(Map<String, Object> payload,
                                               String plainText) {
        String streamId = asString(payload.get("streamId"));
        if (streamId == null || streamId.isBlank()) {
            return defaultString(plainText, "知微正在生成内容。");
        }
        return defaultString(plainText, "知微正在生成内容。") + "\n\n`" + streamId + "`";
    }

    private String renderImageText(ConnectorDeliveryRequest.Content content) {
        Map<String, Object> payload = content.payload() != null ? content.payload() : Map.of();
        String imageUrl = asString(payload.get("imageUrl"));
        String caption = asString(payload.get("caption"));
        if (imageUrl != null && caption != null) {
            return caption + "\n" + imageUrl;
        }
        if (imageUrl != null) {
            return imageUrl;
        }
        return defaultString(content.plainText(), "[图片]");
    }

    private String renderFileText(ConnectorDeliveryRequest.Content content,
                                  List<ConnectorDeliveryRequest.Attachment> attachments) {
        Map<String, Object> payload = content.payload() != null ? content.payload() : Map.of();
        String fileName = firstNonBlank(
                asString(payload.get("fileName")),
                attachments.isEmpty() ? null : attachments.getFirst().fileName()
        );
        if (fileName != null) {
            return "文件: " + fileName;
        }
        String fileUrl = firstNonBlank(asString(payload.get("fileUrl")), asString(payload.get("url")));
        if (fileUrl != null) {
            return fileUrl;
        }
        return defaultString(content.plainText(), "[文件]");
    }

    private String renderImageMarkdownBody(Map<String, Object> payload,
                                           String plainText) {
        String imageUrl = asString(payload.get("imageUrl"));
        if (imageUrl == null || imageUrl.isBlank()) {
            return plainText;
        }
        String caption = asString(payload.get("caption"));
        if (caption != null && !caption.isBlank()) {
            return caption + "\n\n[查看图片](" + imageUrl + ")";
        }
        return "[查看图片](" + imageUrl + ")";
    }

    private String renderFileMarkdownBody(Map<String, Object> payload,
                                          String plainText) {
        String fileName = asString(payload.get("fileName"));
        String fileUrl = firstNonBlank(asString(payload.get("fileUrl")), asString(payload.get("url")));
        if (fileUrl == null || fileUrl.isBlank()) {
            return plainText;
        }
        if (fileName != null && !fileName.isBlank()) {
            return "[下载文件: " + fileName + "](" + fileUrl + ")";
        }
        return "[下载文件](" + fileUrl + ")";
    }

    private String buildCardActionText(Map<String, Object> payload) {
        Map<String, Object> action = nestedMap(payload.get("action"));
        String label = firstNonBlank(
                asString(action.get("label")),
                asString(action.get("name"))
        );
        if (label != null) {
            return "触发了卡片动作: " + label;
        }
        String tag = asString(action.get("tag"));
        if (tag != null) {
            return "触发了卡片动作: " + tag;
        }
        return "触发了卡片动作";
    }

    /**
     * 从卡片回调 payload 中提取 action 标识。
     *
     * <p>优先取 {@code action.name}（回调按钮的 name 字段），
     * 回退取 {@code action.value.action}。知微后端依赖此值识别审批回调。</p>
     */
    @Nullable
    private String resolveCardActionName(Map<String, Object> payload) {
        Map<String, Object> action = nestedMap(payload.get("action"));
        String name = asString(action.get("name"));
        if (name != null && !name.isBlank()) {
            return name;
        }
        Map<String, Object> value = nestedMap(action.get("value"));
        String valueAction = asString(value.get("action"));
        if (valueAction != null && !valueAction.isBlank()) {
            return valueAction;
        }
        return null;
    }

    private String resolveFileKey(ConnectorDeliveryRequest.Content content,
                                  List<ConnectorDeliveryRequest.Attachment> attachments,
                                  FeishuInstanceState state) throws Exception {
        Map<String, Object> payload = content.payload() != null ? content.payload() : Map.of();
        String fileKey = asString(payload.get("fileKey"));
        if (fileKey != null) {
            return fileKey;
        }

        ConnectorDeliveryRequest.Attachment attachment = selectFileAttachment(attachments);
        if (attachment != null) {
            return uploadFileFromAttachment(state, attachment);
        }

        String fileBase64 = firstNonBlank(
                asString(payload.get("fileBase64")),
                asString(payload.get("base64Data"))
        );
        if (fileBase64 != null) {
            byte[] data = decodeBase64(fileBase64);
            verifyUploadSize(data.length, "文件");
            return state.uploadFile(new FeishuFileUpload(
                    asString(payload.get("fileName")),
                    asString(payload.get("mimeType")),
                    data
            ));
        }

        String fileUrl = firstNonBlank(asString(payload.get("fileUrl")), asString(payload.get("url")));
        if (fileUrl != null) {
            return uploadFileFromUrl(state, fileUrl, asString(payload.get("fileName")), asString(payload.get("mimeType")));
        }

        throw new IllegalArgumentException("文件消息缺少可上传的文件源");
    }

    private String resolveImageKey(ConnectorDeliveryRequest.Content content,
                                   List<ConnectorDeliveryRequest.Attachment> attachments,
                                   FeishuInstanceState state) throws Exception {
        Map<String, Object> payload = content.payload() != null ? content.payload() : Map.of();
        String imageKey = asString(payload.get("imageKey"));
        if (imageKey != null) {
            return imageKey;
        }

        ConnectorDeliveryRequest.Attachment imageAttachment = selectImageAttachment(attachments);
        if (imageAttachment != null) {
            return uploadImageFromAttachment(state, imageAttachment);
        }

        String imageBase64 = firstNonBlank(
                asString(payload.get("imageBase64")),
                asString(payload.get("base64Data"))
        );
        if (imageBase64 != null) {
            byte[] data = decodeBase64(imageBase64);
            verifyUploadSize(data.length, "图片");
            return state.uploadImage(new FeishuImageUpload(
                    asString(payload.get("fileName")),
                    asString(payload.get("mimeType")),
                    data
            ));
        }

        String imageUrl = asString(payload.get("imageUrl"));
        if (imageUrl != null) {
            return uploadImageFromUrl(state, imageUrl, asString(payload.get("fileName")));
        }

        throw new IllegalArgumentException("图片消息缺少可上传的图片源");
    }

    private String uploadImageFromAttachment(FeishuInstanceState state,
                                             ConnectorDeliveryRequest.Attachment attachment) throws Exception {
        byte[] data = decodeBase64(attachment.base64Data());
        verifyUploadSize(data.length, "图片");
        return state.uploadImage(new FeishuImageUpload(
                attachment.fileName(),
                attachment.mimeType(),
                data
        ));
    }

    private String uploadImageFromUrl(FeishuInstanceState state,
                                      String imageUrl,
                                      @Nullable String fileName) throws Exception {
        if (imageUrl.startsWith("data:")) {
            DataUriImage dataUriImage = parseDataUri(imageUrl);
            verifyUploadSize(dataUriImage.data().length, "图片");
            return state.uploadImage(new FeishuImageUpload(
                    firstNonBlank(fileName, "image" + guessSuffix(dataUriImage.mimeType())),
                    dataUriImage.mimeType(),
                    dataUriImage.data()
            ));
        }

        HttpRequest request = HttpRequest.newBuilder(URI.create(imageUrl))
                .GET()
                .build();
        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException("下载图片失败: status=" + response.statusCode());
        }
        byte[] data = response.body() != null ? response.body() : new byte[0];
        verifyUploadSize(data.length, "图片");
        String mimeType = response.headers().firstValue("Content-Type").orElse(null);
        return state.uploadImage(new FeishuImageUpload(
                firstNonBlank(fileName, extractFileName(imageUrl)),
                mimeType,
                data
        ));
    }

    @Nullable
    private ConnectorDeliveryRequest.Attachment selectImageAttachment(List<ConnectorDeliveryRequest.Attachment> attachments) {
        for (ConnectorDeliveryRequest.Attachment attachment : attachments) {
            String mimeType = attachment.mimeType();
            if (mimeType != null && mimeType.toLowerCase().startsWith("image/")) {
                return attachment;
            }
            String fileName = attachment.fileName();
            if (fileName != null && fileName.matches("(?i).+\\.(png|jpg|jpeg|gif|webp|bmp)$")) {
                return attachment;
            }
        }
        return null;
    }

    @Nullable
    private ConnectorDeliveryRequest.Attachment selectFileAttachment(List<ConnectorDeliveryRequest.Attachment> attachments) {
        if (attachments.isEmpty()) {
            return null;
        }
        for (ConnectorDeliveryRequest.Attachment attachment : attachments) {
            String mimeType = attachment.mimeType();
            if (mimeType == null || !mimeType.toLowerCase().startsWith("image/")) {
                return attachment;
            }
        }
        return attachments.getFirst();
    }

    private String uploadFileFromAttachment(FeishuInstanceState state,
                                            ConnectorDeliveryRequest.Attachment attachment) throws Exception {
        byte[] data = decodeBase64(attachment.base64Data());
        verifyUploadSize(data.length, "文件");
        return state.uploadFile(new FeishuFileUpload(
                attachment.fileName(),
                attachment.mimeType(),
                data
        ));
    }

    private String uploadFileFromUrl(FeishuInstanceState state,
                                     String fileUrl,
                                     @Nullable String fileName,
                                     @Nullable String mimeType) throws Exception {
        if (fileUrl.startsWith("data:")) {
            DataUriImage dataUriImage = parseDataUri(fileUrl);
            verifyUploadSize(dataUriImage.data().length, "文件");
            return state.uploadFile(new FeishuFileUpload(
                    firstNonBlank(fileName, "file" + guessSuffix(dataUriImage.mimeType())),
                    firstNonBlank(mimeType, dataUriImage.mimeType()),
                    dataUriImage.data()
            ));
        }

        HttpRequest request = HttpRequest.newBuilder(URI.create(fileUrl))
                .GET()
                .build();
        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException("下载文件失败: status=" + response.statusCode());
        }
        byte[] data = response.body() != null ? response.body() : new byte[0];
        verifyUploadSize(data.length, "文件");
        String responseMimeType = response.headers().firstValue("Content-Type").orElse(null);
        return state.uploadFile(new FeishuFileUpload(
                firstNonBlank(fileName, extractFileName(fileUrl)),
                firstNonBlank(mimeType, responseMimeType),
                data
        ));
    }

    private byte[] decodeBase64(String data) {
        try {
            return Base64.getDecoder().decode(data);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("图片 base64 数据无效", ex);
        }
    }

    private void verifyUploadSize(int size, String kind) {
        if (size <= 0) {
            throw new IllegalArgumentException(kind + "内容为空");
        }
        if (size > imageMaxBytes) {
            throw new IllegalArgumentException(kind + "大小超过限制: " + size);
        }
    }

    private DataUriImage parseDataUri(String imageUrl) {
        int commaIndex = imageUrl.indexOf(',');
        if (!imageUrl.startsWith("data:") || commaIndex < 0) {
            throw new IllegalArgumentException("不支持的 data URI");
        }
        String metadata = imageUrl.substring(5, commaIndex);
        if (!metadata.endsWith(";base64")) {
            throw new IllegalArgumentException("当前仅支持 base64 data URI");
        }
        String mimeType = metadata.substring(0, metadata.length() - ";base64".length());
        String base64Data = imageUrl.substring(commaIndex + 1);
        return new DataUriImage(
                mimeType.isBlank() ? null : mimeType,
                decodeBase64(base64Data)
        );
    }

    @Nullable
    private String extractFileName(String imageUrl) {
        try {
            String path = URI.create(imageUrl).getPath();
            if (path == null || path.isBlank()) {
                return null;
            }
            int lastSlash = path.lastIndexOf('/');
            String fileName = lastSlash >= 0 ? path.substring(lastSlash + 1) : path;
            return fileName.isBlank() ? null : fileName;
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    private String guessSuffix(@Nullable String mimeType) {
        if (mimeType == null) {
            return ".img";
        }
        return switch (mimeType.toLowerCase()) {
            case "image/png" -> ".png";
            case "image/jpeg", "image/jpg" -> ".jpg";
            case "image/gif" -> ".gif";
            case "image/webp" -> ".webp";
            case "image/bmp" -> ".bmp";
            default -> ".img";
        };
    }

    @Nullable
    private String extractFirstDeliveryText(List<Map<String, Object>> deliveries) {
        for (Map<String, Object> rawDelivery : deliveries) {
            Object content = rawDelivery.get("content");
            if (content instanceof Map<?, ?> map) {
                String plainText = asString(map.get("plainText"));
                if (plainText != null) {
                    return plainText;
                }
                String text = asString(map.get("text"));
                if (text != null) {
                    return text;
                }
                Object payload = map.get("payload");
                if (payload instanceof Map<?, ?> payloadMap) {
                    String body = firstNonBlank(
                            asString(payloadMap.get("body")),
                            asString(payloadMap.get("title"))
                    );
                    if (body != null) {
                        return body;
                    }
                }
            }
        }
        return null;
    }

    private String truncateToast(String text) {
        String normalized = text.replace('\n', ' ').trim();
        if (normalized.length() <= 60) {
            return normalized;
        }
        return normalized.substring(0, 57) + "...";
    }

    private Map<String, Object> normalizeMap(Map<?, ?> source) {
        LinkedHashMap<String, Object> normalized = new LinkedHashMap<>();
        source.forEach((key, value) -> {
            if (key != null) {
                normalized.put(String.valueOf(key), normalizeValue(value));
            }
        });
        return Map.copyOf(normalized);
    }

    private List<Object> normalizeList(List<?> source) {
        List<Object> normalized = new ArrayList<>(source.size());
        for (Object item : source) {
            normalized.add(normalizeValue(item));
        }
        return List.copyOf(normalized);
    }

    private Object normalizeValue(@Nullable Object value) {
        if (value instanceof Map<?, ?> map) {
            return normalizeMap(map);
        }
        if (value instanceof List<?> list) {
            return normalizeList(list);
        }
        return value;
    }

    @Nullable
    private Map<String, Object> nestedMap(@Nullable Object value) {
        if (value instanceof Map<?, ?> map) {
            return normalizeMap(map);
        }
        return Map.of();
    }

    private boolean supportsTextOrPostUpdate(String msgType) {
        return "text".equalsIgnoreCase(msgType) || "post".equalsIgnoreCase(msgType);
    }

    private String serializeTextContent(String text) {
        return writeJson(Map.of("text", text));
    }

    private String writeJson(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("飞书消息内容序列化失败", ex);
        }
    }

    private Map<String, Object> mergeMetadata(FeishuInstanceState state,
                                              FeishuInboundMessage message) {
        LinkedHashMap<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("source", "feishu-connector");
        metadata.put("platform", state.platform());
        metadata.put("pluginId", state.pluginId());
        putIfNotBlank(metadata, "messageType", message.messageType());
        putIfNotBlank(metadata, "chatId", message.chatId());
        putIfNotBlank(metadata, "chatType", message.chatType());
        putIfNotBlank(metadata, "threadId", message.threadId());
        putIfNotBlank(metadata, "rootId", message.rootId());
        putIfNotBlank(metadata, "parentId", message.parentId());
        putIfNotBlank(metadata, "senderType", message.senderType());
        putIfNotBlank(metadata, "senderOpenId", message.senderOpenId());
        putIfNotBlank(metadata, "senderUserId", message.senderUserId());
        putIfNotBlank(metadata, "senderUnionId", message.senderUnionId());
        putIfNotBlank(metadata, "tenantKey", message.tenantKey());
        putIfNotBlank(metadata, "rawContent", message.content());
        return Map.copyOf(metadata);
    }

    private Map<String, Object> buildReplyAttributes(FeishuInboundMessage message) {
        LinkedHashMap<String, Object> attributes = new LinkedHashMap<>();
        putIfNotBlank(attributes, "messageId", message.messageId());
        putIfNotBlank(attributes, "chatId", message.chatId());
        putIfNotBlank(attributes, "threadId", message.threadId());
        putIfNotBlank(attributes, "rootId", message.rootId());
        putIfNotBlank(attributes, "parentId", message.parentId());
        putIfNotBlank(attributes, "tenantKey", message.tenantKey());
        putIfNotBlank(attributes, "senderOpenId", message.senderOpenId());
        putIfNotBlank(attributes, "senderUserId", message.senderUserId());
        putIfNotBlank(attributes, "senderUnionId", message.senderUnionId());
        if (message.threadId() != null && !message.threadId().isBlank()) {
            attributes.put("replyInThread", true);
        }
        return Map.copyOf(attributes);
    }

    private String resolveUserId(FeishuInboundMessage message) {
        if (message.senderOpenId() != null && !message.senderOpenId().isBlank()) {
            return message.senderOpenId().trim();
        }
        if (message.senderUserId() != null && !message.senderUserId().isBlank()) {
            return message.senderUserId().trim();
        }
        throw new IllegalArgumentException("飞书事件缺少发送者标识");
    }

    private String extractText(String messageType,
                               @Nullable JsonNode contentJson,
                               @Nullable String rawContent) {
        String type = messageType != null ? messageType.trim().toLowerCase() : "text";
        if (contentJson == null) {
            return defaultFallbackText(type, rawContent);
        }
        if ("text".equals(type)) {
            String value = readTextField(contentJson, "text");
            return value != null ? value : defaultFallbackText(type, rawContent);
        }
        String recursiveText = collectText(contentJson);
        if (recursiveText != null && !recursiveText.isBlank()) {
            return recursiveText;
        }
        return defaultFallbackText(type, rawContent);
    }

    private String defaultFallbackText(String type, @Nullable String rawContent) {
        return switch (type) {
            case "image" -> "[图片]";
            case "audio" -> "[音频]";
            case "media" -> "[媒体]";
            case "file" -> "[文件]";
            case "sticker" -> "[表情]";
            case "interactive" -> "[卡片]";
            default -> rawContent != null ? rawContent : "";
        };
    }

    private Map<String, Object> extractPayload(@Nullable JsonNode contentJson,
                                               @Nullable String rawContent) {
        if (contentJson != null && contentJson.isObject()) {
            return objectMapper.convertValue(contentJson, MAP_TYPE);
        }
        if (rawContent == null || rawContent.isBlank()) {
            return Map.of();
        }
        return Map.of("raw", rawContent);
    }

    @Nullable
    private JsonNode parseJson(@Nullable String rawContent) {
        if (rawContent == null || rawContent.isBlank()) {
            return null;
        }
        try {
            return objectMapper.readTree(rawContent);
        } catch (JsonProcessingException ex) {
            return null;
        }
    }

    @Nullable
    private String readTextField(JsonNode node, String fieldName) {
        JsonNode field = node.path(fieldName);
        return field.isTextual() ? field.asText() : null;
    }

    @Nullable
    private String collectText(JsonNode node) {
        List<String> values = new ArrayList<>();
        collectText(node, values);
        if (values.isEmpty()) {
            return null;
        }
        return values.stream()
                .filter(value -> value != null && !value.isBlank())
                .collect(Collectors.joining("\n"));
    }

    private void collectText(JsonNode node, List<String> values) {
        if (node == null || node.isNull()) {
            return;
        }
        if (node.isObject()) {
            node.fields().forEachRemaining(entry -> {
                if ("tag".equals(entry.getKey()) || "href".equals(entry.getKey())) {
                    return;
                }
                if ("text".equals(entry.getKey()) && entry.getValue().isTextual()) {
                    values.add(entry.getValue().asText());
                    return;
                }
                collectText(entry.getValue(), values);
            });
            return;
        }
        if (node.isArray()) {
            node.forEach(child -> collectText(child, values));
            return;
        }
        if (node.isTextual()) {
            values.add(node.asText());
        }
    }

    private void validateConnectionMode(String connectionMode) {
        if (!"websocket".equalsIgnoreCase(connectionMode) && !"webhook".equalsIgnoreCase(connectionMode)) {
            throw new IllegalArgumentException("当前飞书 connector 仅支持 websocket 或 webhook 模式");
        }
    }

    @Nullable
    private String asString(@Nullable Object raw) {
        if (raw instanceof String value && !value.isBlank()) {
            return value.trim();
        }
        return null;
    }

    private boolean asBoolean(@Nullable Object raw) {
        return raw instanceof Boolean value && value;
    }

    @Nullable
    private String firstNonBlank(@Nullable String first, @Nullable String second) {
        if (first != null && !first.isBlank()) {
            return first;
        }
        if (second != null && !second.isBlank()) {
            return second;
        }
        return null;
    }

    private String defaultString(@Nullable String value, String defaultValue) {
        return value != null ? value : defaultValue;
    }

    private void putIfNotBlank(Map<String, Object> payload, String key, @Nullable String value) {
        if (value != null && !value.isBlank()) {
            payload.put(key, value.trim());
        }
    }

    private FeishuInstanceState requireState(String instanceId) {
        FeishuInstanceState state = instances.get(instanceId);
        if (state == null) {
            throw new IllegalArgumentException("实例尚未启动: " + instanceId);
        }
        return state;
    }

    private record DataUriImage(
            @Nullable String mimeType,
            byte[] data
    ) {
    }

    static final class FeishuInstanceState {

        private final String instanceId;
        private final String runtimeToken;
        private String pluginId = "feishu";
        private String platform = "feishu";
        private Map<String, Object> config = Map.of();
        private Map<String, Object> secretConfig = Map.of();
        private Map<String, Object> routingPolicy = Map.of();
        private String connectionMode = "websocket";
        @Nullable
        private String verificationToken;
        private boolean running;
        private int deliveryCount;
        private int inboundEventCount;
        private int ignoredEventCount;
        private int callbackFailureCount;
        private final Map<String, FeishuSentMessageRef> responseMessages = new LinkedHashMap<>();
        @Nullable
        private Instant lastStartedAt;
        @Nullable
        private Instant lastStoppedAt;
        @Nullable
        private Instant lastReloadedAt;
        @Nullable
        private Instant lastDeliveredAt;
        @Nullable
        private Instant lastInboundEventAt;
        @Nullable
        private Instant lastHeartbeatForwardedAt;
        @Nullable
        private String lastResponseId;
        @Nullable
        private String lastMessageId;
        @Nullable
        private String lastChatId;
        @Nullable
        private String lastError;
        @Nullable
        private FeishuPlatformSession session;

        FeishuInstanceState(String instanceId, String runtimeToken) {
            this.instanceId = instanceId;
            this.runtimeToken = runtimeToken;
        }

        synchronized void start(ConnectorInstanceCommandRequest request,
                                FeishuInstanceSettings settings,
                                FeishuPlatformSession session) {
            apply(request, settings, session);
            try {
                session.start();
                running = true;
                lastStartedAt = Instant.now();
                lastError = null;
            } catch (RuntimeException ex) {
                running = false;
                lastError = ex.getMessage();
                stopQuietly();
                throw ex;
            }
        }

        synchronized void stop() {
            stopQuietly();
            running = false;
            lastStoppedAt = Instant.now();
        }

        synchronized void reload(ConnectorInstanceCommandRequest request,
                                 FeishuInstanceSettings settings,
                                 FeishuPlatformSession session) {
            stopQuietly();
            apply(request, settings, session);
            try {
                session.start();
                running = true;
                lastReloadedAt = Instant.now();
                lastError = null;
            } catch (RuntimeException ex) {
                running = false;
                lastError = ex.getMessage();
                stopQuietly();
                throw ex;
            }
        }

        synchronized FeishuSendResult send(FeishuOutgoingMessage message) throws Exception {
            if (!running || session == null) {
                throw new IllegalStateException("飞书实例未运行");
            }
            try {
                FeishuSendResult result = session.send(message);
                deliveryCount += 1;
                lastDeliveredAt = Instant.now();
                lastResponseId = message.uuid();
                if (result.messageId() != null && !result.messageId().isBlank()) {
                    lastMessageId = result.messageId();
                }
                if (message.uuid() != null && !message.uuid().isBlank()
                        && result.messageId() != null && !result.messageId().isBlank()) {
                    responseMessages.put(message.uuid(), new FeishuSentMessageRef(result.messageId(), result.msgType()));
                }
                lastError = null;
                return result;
            } catch (Exception ex) {
                lastError = ex.getMessage();
                throw ex;
            }
        }

        synchronized FeishuWebhookResponse handleWebhook(FeishuWebhookRequest request) throws Exception {
            if (!running || session == null) {
                throw new IllegalStateException("飞书实例未运行");
            }
            try {
                FeishuWebhookResponse response = session.handleWebhook(request);
                lastError = null;
                return response;
            } catch (Exception ex) {
                lastError = ex.getMessage();
                throw ex;
            }
        }

        synchronized String uploadImage(FeishuImageUpload image) throws Exception {
            if (!running || session == null) {
                throw new IllegalStateException("飞书实例未运行");
            }
            try {
                return session.uploadImage(image);
            } catch (Exception ex) {
                lastError = ex.getMessage();
                throw ex;
            }
        }

        synchronized String uploadFile(FeishuFileUpload file) throws Exception {
            if (!running || session == null) {
                throw new IllegalStateException("飞书实例未运行");
            }
            try {
                return session.uploadFile(file);
            } catch (Exception ex) {
                lastError = ex.getMessage();
                throw ex;
            }
        }

        synchronized Map<String, Object> updateMessage(String messageId, String msgType, String content) throws Exception {
            if (!running || session == null) {
                throw new IllegalStateException("飞书实例未运行");
            }
            try {
                Map<String, Object> result = session.updateMessage(messageId, msgType, content);
                lastError = null;
                return result;
            } catch (Exception ex) {
                lastError = ex.getMessage();
                throw ex;
            }
        }

        synchronized Map<String, Object> recallMessage(String messageId) throws Exception {
            if (!running || session == null) {
                throw new IllegalStateException("飞书实例未运行");
            }
            try {
                Map<String, Object> result = session.recallMessage(messageId);
                lastError = null;
                return result;
            } catch (Exception ex) {
                lastError = ex.getMessage();
                throw ex;
            }
        }

        synchronized Map<String, Object> downloadResource(String messageId, String fileKey, String type) throws Exception {
            if (!running || session == null) {
                throw new IllegalStateException("飞书实例未运行");
            }
            try {
                Map<String, Object> result = session.downloadResource(messageId, fileKey, type);
                lastError = null;
                return result;
            } catch (Exception ex) {
                lastError = ex.getMessage();
                throw ex;
            }
        }

        synchronized Map<String, Object> createChat(String name, String description, String[] userIdList) throws Exception {
            if (!running || session == null) {
                throw new IllegalStateException("飞书实例未运行");
            }
            try {
                Map<String, Object> result = session.createChat(name, description, userIdList);
                lastError = null;
                return result;
            } catch (Exception ex) {
                lastError = ex.getMessage();
                throw ex;
            }
        }

        synchronized Map<String, Object> addChatMembers(String chatId, String memberIdType, String[] idList) throws Exception {
            if (!running || session == null) {
                throw new IllegalStateException("飞书实例未运行");
            }
            try {
                Map<String, Object> result = session.addChatMembers(chatId, memberIdType, idList);
                lastError = null;
                return result;
            } catch (Exception ex) {
                lastError = ex.getMessage();
                throw ex;
            }
        }

        synchronized Map<String, Object> removeChatMembers(String chatId, String memberIdType, String[] idList) throws Exception {
            if (!running || session == null) {
                throw new IllegalStateException("飞书实例未运行");
            }
            try {
                Map<String, Object> result = session.removeChatMembers(chatId, memberIdType, idList);
                lastError = null;
                return result;
            } catch (Exception ex) {
                lastError = ex.getMessage();
                throw ex;
            }
        }

        synchronized Map<String, Object> createTask(String summary, @Nullable String dueTimestamp) throws Exception {
            if (!running || session == null) {
                throw new IllegalStateException("飞书实例未运行");
            }
            try {
                Map<String, Object> result = session.createTask(summary, dueTimestamp);
                lastError = null;
                return result;
            } catch (Exception ex) {
                lastError = ex.getMessage();
                throw ex;
            }
        }

        synchronized Map<String, Object> createDocument(String title, @Nullable String folderToken) throws Exception {
            if (!running || session == null) {
                throw new IllegalStateException("飞书实例未运行");
            }
            try {
                Map<String, Object> result = session.createDocument(title, folderToken);
                lastError = null;
                return result;
            } catch (Exception ex) {
                lastError = ex.getMessage();
                throw ex;
            }
        }

        synchronized Map<String, Object> createCalendarEvent(String calendarId, String summary,
                                                              String startTime, String endTime) throws Exception {
            if (!running || session == null) {
                throw new IllegalStateException("飞书实例未运行");
            }
            try {
                Map<String, Object> result = session.createCalendarEvent(calendarId, summary, startTime, endTime);
                lastError = null;
                return result;
            } catch (Exception ex) {
                lastError = ex.getMessage();
                throw ex;
            }
        }

        @Nullable
        synchronized FeishuSentMessageRef findResponseMessage(@Nullable String responseId) {
            if (responseId == null || responseId.isBlank()) {
                return null;
            }
            return responseMessages.get(responseId);
        }

        /**
         * 删除已记录的 responseId → messageId 映射 — 流式 flush 命中 404/消息撤回时清理，
         * 让下次同 rid 的 flush 走 send 路径重新建立映射。
         */
        synchronized void removeResponseMessage(@Nullable String responseId) {
            if (responseId == null || responseId.isBlank()) {
                return;
            }
            responseMessages.remove(responseId);
        }

        @Nullable
        synchronized String lastChatId() {
            return lastChatId;
        }

        synchronized void recordInboundEvent(FeishuInboundMessage message, @Nullable String responseId) {
            inboundEventCount += 1;
            lastInboundEventAt = Instant.now();
            lastResponseId = responseId;
            lastMessageId = message.messageId();
            lastChatId = message.chatId();
            lastError = null;
        }

        synchronized void recordIgnoredEvent(String message) {
            ignoredEventCount += 1;
            lastError = message;
        }

        synchronized void recordCallbackFailure(RuntimeException ex) {
            callbackFailureCount += 1;
            lastError = ex.getMessage();
        }

        synchronized void recordCallbackRejection(@Nullable String errorMessage) {
            lastError = errorMessage != null ? errorMessage : "知微拒绝了飞书事件";
        }

        synchronized void recordDeliveryFailure(Exception ex) {
            lastError = ex.getMessage();
        }

        synchronized void recordHeartbeat() {
            lastHeartbeatForwardedAt = Instant.now();
        }

        synchronized void stopQuietly() {
            if (session != null) {
                try {
                    session.stop();
                } catch (RuntimeException ex) {
                    lastError = ex.getMessage();
                } finally {
                    session = null;
                }
            }
        }

        synchronized Map<String, Object> commandResult(String status, String message) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("instanceId", instanceId);
            payload.put("status", status);
            payload.put("message", message);
            payload.put("platform", platform);
            payload.put("pluginId", pluginId);
            payload.put("running", running);
            payload.put("connectionMode", connectionMode);
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
            payload.put("connectionMode", connectionMode);
            payload.put("supportedModes", List.of("websocket", "webhook"));
            payload.put("deliveryCount", deliveryCount);
            payload.put("inboundEventCount", inboundEventCount);
            payload.put("ignoredEventCount", ignoredEventCount);
            payload.put("callbackFailureCount", callbackFailureCount);
            payload.put("trackedResponseCount", responseMessages.size());
            putIfNotNull(payload, "lastStartedAt", lastStartedAt);
            putIfNotNull(payload, "lastStoppedAt", lastStoppedAt);
            putIfNotNull(payload, "lastReloadedAt", lastReloadedAt);
            putIfNotNull(payload, "lastDeliveredAt", lastDeliveredAt);
            putIfNotNull(payload, "lastEventAt", lastInboundEventAt);
            putIfNotNull(payload, "lastHeartbeatForwardedAt", lastHeartbeatForwardedAt);
            putIfNotNull(payload, "lastResponseId", lastResponseId);
            putIfNotNull(payload, "lastMessageId", lastMessageId);
            putIfNotNull(payload, "lastChatId", lastChatId);
            putIfNotNull(payload, "lastError", lastError);
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

        boolean running() {
            return running;
        }

        boolean webhookMode() {
            return "webhook".equalsIgnoreCase(connectionMode);
        }

        @Nullable
        String verificationToken() {
            return verificationToken;
        }

        int deliveryCount() {
            return deliveryCount;
        }

        @Nullable
        Instant lastDeliveredAt() {
            return lastDeliveredAt;
        }

        String instanceId() {
            return instanceId;
        }

        private void apply(ConnectorInstanceCommandRequest request,
                           FeishuInstanceSettings settings,
                           FeishuPlatformSession session) {
            pluginId = request.pluginId();
            platform = request.platform();
            config = request.config();
            secretConfig = request.secretConfig() != null ? request.secretConfig() : Map.of();
            routingPolicy = request.routingPolicy() != null ? request.routingPolicy() : Map.of();
            connectionMode = settings.connectionMode();
            verificationToken = settings.verificationToken();
            responseMessages.clear();
            this.session = session;
        }

        private void putIfNotNull(Map<String, Object> payload, String key, @Nullable Object value) {
            if (value != null) {
                payload.put(key, value);
            }
        }
    }

    /**
     * 根据飞书 msgType + fileName 推断真实 MIME。
     *
     * <p>飞书 msgType=file 的普通文件下载接口不返回 MIME，原实现硬编码
     * {@code application/octet-stream}，导致主服务文档 parser 路由失败、附件上下文
     * 注入白名单匹配失败。这里按 fileName 扩展名兜底推断，让 connector 上报的 MIME
     * 真实反映文件类型。</p>
     *
     * <p>覆盖范围：docx/xlsx/pptx/pdf/md/csv/tsv/txt/json/xml/html 和常见媒体类型。
     * 未知扩展名保持 {@code application/octet-stream}，交给主服务做进一步判断。</p>
     */
    static String inferInboundMimeType(String msgType, String fileName) {
        if ("audio".equals(msgType)) return "audio/ogg";
        if ("media".equals(msgType)) return "video/mp4";
        if (fileName == null) return "application/octet-stream";
        String lower = fileName.toLowerCase();
        int dot = lower.lastIndexOf('.');
        if (dot < 0 || dot == lower.length() - 1) return "application/octet-stream";
        String ext = lower.substring(dot + 1);
        return switch (ext) {
            case "docx" -> "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
            case "xlsx" -> "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
            case "pptx" -> "application/vnd.openxmlformats-officedocument.presentationml.presentation";
            case "doc" -> "application/msword";
            case "xls" -> "application/vnd.ms-excel";
            case "ppt" -> "application/vnd.ms-powerpoint";
            case "pdf" -> "application/pdf";
            case "md", "markdown", "mkd" -> "text/markdown";
            case "csv" -> "text/csv";
            case "tsv" -> "text/tab-separated-values";
            case "txt", "text", "log" -> "text/plain";
            case "json" -> "application/json";
            case "xml" -> "application/xml";
            case "html", "htm" -> "text/html";
            case "zip" -> "application/zip";
            case "gz", "gzip" -> "application/gzip";
            case "tar" -> "application/x-tar";
            case "mp3" -> "audio/mpeg";
            case "wav" -> "audio/wav";
            case "mp4" -> "video/mp4";
            case "mov" -> "video/quicktime";
            case "avi" -> "video/x-msvideo";
            case "png" -> "image/png";
            case "jpg", "jpeg" -> "image/jpeg";
            case "gif" -> "image/gif";
            case "webp" -> "image/webp";
            case "svg" -> "image/svg+xml";
            default -> "application/octet-stream";
        };
    }
}
