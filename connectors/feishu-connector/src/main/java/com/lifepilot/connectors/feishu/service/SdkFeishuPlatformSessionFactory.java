package com.lifepilot.connectors.feishu.service;

import com.lark.oapi.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.lark.oapi.core.request.EventReq;
import com.lark.oapi.core.response.EventResp;
import com.lark.oapi.core.utils.Jsons;
import com.lark.oapi.event.EventDispatcher;
import com.lark.oapi.event.cardcallback.P2CardActionTriggerHandler;
import com.lark.oapi.event.cardcallback.model.CallBackAction;
import com.lark.oapi.event.cardcallback.model.CallBackContext;
import com.lark.oapi.event.cardcallback.model.CallBackOperator;
import com.lark.oapi.event.cardcallback.model.CallBackToast;
import com.lark.oapi.event.cardcallback.model.P2CardActionTrigger;
import com.lark.oapi.event.cardcallback.model.P2CardActionTriggerData;
import com.lark.oapi.event.cardcallback.model.P2CardActionTriggerResponse;
import com.lark.oapi.event.model.Header;
import com.lark.oapi.service.im.ImService;
import com.lark.oapi.service.calendar.v4.model.CalendarEvent;
import com.lark.oapi.service.calendar.v4.model.CreateCalendarEventReq;
import com.lark.oapi.service.calendar.v4.model.CreateCalendarEventResp;
import com.lark.oapi.service.calendar.v4.model.TimeInfo;
import com.lark.oapi.service.docx.v1.model.CreateDocumentReq;
import com.lark.oapi.service.docx.v1.model.CreateDocumentReqBody;
import com.lark.oapi.service.docx.v1.model.CreateDocumentResp;
import com.lark.oapi.service.im.v1.model.CreateChatMembersReq;
import com.lark.oapi.service.im.v1.model.CreateChatMembersReqBody;
import com.lark.oapi.service.im.v1.model.CreateChatMembersResp;
import com.lark.oapi.service.im.v1.model.CreateChatReq;
import com.lark.oapi.service.im.v1.model.CreateChatReqBody;
import com.lark.oapi.service.im.v1.model.CreateChatResp;
import com.lark.oapi.service.im.v1.model.CreateFileReq;
import com.lark.oapi.service.im.v1.model.CreateFileReqBody;
import com.lark.oapi.service.im.v1.model.CreateFileResp;
import com.lark.oapi.service.im.v1.model.CreateImageReq;
import com.lark.oapi.service.im.v1.model.CreateImageReqBody;
import com.lark.oapi.service.im.v1.model.CreateImageResp;
import com.lark.oapi.service.im.v1.model.CreateMessageReq;
import com.lark.oapi.service.im.v1.model.CreateMessageReqBody;
import com.lark.oapi.service.im.v1.model.CreateMessageResp;
import com.lark.oapi.service.im.v1.model.DeleteChatMembersReq;
import com.lark.oapi.service.im.v1.model.DeleteChatMembersReqBody;
import com.lark.oapi.service.im.v1.model.DeleteChatMembersResp;
import com.lark.oapi.service.im.v1.model.DeleteMessageReq;
import com.lark.oapi.service.im.v1.model.DeleteMessageResp;
import com.lark.oapi.service.im.v1.model.GetMessageResourceReq;
import com.lark.oapi.service.im.v1.model.GetMessageResourceResp;
import com.lark.oapi.service.im.v1.model.PatchMessageReq;
import com.lark.oapi.service.im.v1.model.PatchMessageReqBody;
import com.lark.oapi.service.im.v1.model.PatchMessageResp;
import com.lark.oapi.service.im.v1.model.P2MessageReceiveV1;
import com.lark.oapi.service.im.v1.model.P2MessageReceiveV1Data;
import com.lark.oapi.service.im.v1.model.ReplyMessageReq;
import com.lark.oapi.service.im.v1.model.ReplyMessageReqBody;
import com.lark.oapi.service.im.v1.model.ReplyMessageResp;
import com.lark.oapi.service.im.v1.model.UpdateMessageReq;
import com.lark.oapi.service.im.v1.model.UpdateMessageReqBody;
import com.lark.oapi.service.im.v1.model.UpdateMessageResp;
import com.lark.oapi.service.im.v1.model.UserId;
import com.lark.oapi.service.task.v2.model.CreateTaskReq;
import com.lark.oapi.service.task.v2.model.CreateTaskResp;
import com.lark.oapi.service.task.v2.model.Due;
import com.lark.oapi.service.task.v2.model.InputTask;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/**
 * 基于飞书官方 SDK 的平台会话工厂。
 *
 * @author zsg
 * @since 2026-03-29
 */
@Component
public class SdkFeishuPlatformSessionFactory implements FeishuPlatformSessionFactory {

    private static final Logger log = LoggerFactory.getLogger(SdkFeishuPlatformSessionFactory.class);

    @Override
    public FeishuPlatformSession create(FeishuInstanceSettings settings,
                                        Function<FeishuInboundMessage, FeishuInboundHandlingResult> inboundHandler) {
        return new SdkFeishuPlatformSession(settings, inboundHandler);
    }

    static final class SdkFeishuPlatformSession implements FeishuPlatformSession {

        private final FeishuInstanceSettings settings;
        private final Function<FeishuInboundMessage, FeishuInboundHandlingResult> inboundHandler;
        private final Client apiClient;
        private final EventDispatcher eventDispatcher;
        @Nullable
        private com.lark.oapi.ws.Client wsClient;

        SdkFeishuPlatformSession(FeishuInstanceSettings settings,
                                 Function<FeishuInboundMessage, FeishuInboundHandlingResult> inboundHandler) {
            this.settings = settings;
            this.inboundHandler = inboundHandler;
            this.apiClient = Client.newBuilder(settings.appId(), settings.appSecret()).build();
            this.eventDispatcher = buildEventDispatcher();
        }

        @Override
        public synchronized void start() {
            stop();
            if ("webhook".equalsIgnoreCase(settings.connectionMode())) {
                return;
            }
            wsClient = new com.lark.oapi.ws.Client.Builder(settings.appId(), settings.appSecret())
                    .eventHandler(eventDispatcher)
                    .autoReconnect(true)
                    .build();
            wsClient.start();
        }

        @Override
        public synchronized void stop() {
            if (wsClient == null) {
                return;
            }
            try {
                Method disconnect = com.lark.oapi.ws.Client.class.getDeclaredMethod("disconnect");
                disconnect.setAccessible(true);
                disconnect.invoke(wsClient);
            } catch (ReflectiveOperationException ignored) {
            }
            try {
                Field executorField = com.lark.oapi.ws.Client.class.getDeclaredField("executor");
                executorField.setAccessible(true);
                Object executor = executorField.get(wsClient);
                if (executor instanceof ExecutorService executorService) {
                    executorService.shutdownNow();
                }
            } catch (ReflectiveOperationException ignored) {
            } finally {
                wsClient = null;
            }
        }

        @Override
        public FeishuSendResult send(FeishuOutgoingMessage message) throws Exception {
            return switch (message.route()) {
                case CREATE -> create(message);
                case REPLY -> reply(message);
                case UPDATE -> update(message);
                case PATCH -> patch(message);
            };
        }

        @Override
        public FeishuWebhookResponse handleWebhook(FeishuWebhookRequest request) throws Exception {
            EventReq eventReq = new EventReq();
            eventReq.setHttpPath(request.httpPath());
            eventReq.setBody(request.body());
            eventReq.setHeaders(request.headers());

            EventResp response;
            try {
                response = eventDispatcher.handle(eventReq);
            } catch (Exception ex) {
                throw ex;
            } catch (Throwable throwable) {
                throw new IllegalStateException("飞书 webhook 处理失败", throwable);
            }
            Map<String, List<String>> headers = response.getHeaders() != null
                    ? Map.copyOf(response.getHeaders())
                    : Map.of();
            byte[] body = response.getBody() != null ? response.getBody() : new byte[0];
            return new FeishuWebhookResponse(response.getStatusCode(), body, headers);
        }

        @Override
        public String uploadImage(FeishuImageUpload image) throws Exception {
            Path tempFile = Files.createTempFile("feishu-image-", resolveFileSuffix(image));
            try {
                Files.write(tempFile, image.data());
                CreateImageReq request = CreateImageReq.newBuilder()
                        .createImageReqBody(CreateImageReqBody.newBuilder()
                                .imageType("message")
                                .image(tempFile.toFile())
                                .build())
                        .build();
                CreateImageResp response = apiClient.im().image().create(request);
                assertResponseSuccess(response.getCode(), response.getMsg());
                String imageKey = response.getData() != null ? response.getData().getImageKey() : null;
                if (imageKey == null || imageKey.isBlank()) {
                    throw new IllegalStateException("飞书图片上传成功但未返回 image_key");
                }
                return imageKey;
            } finally {
                Files.deleteIfExists(tempFile);
            }
        }

        @Override
        public String uploadFile(FeishuFileUpload file) throws Exception {
            Path tempFile = Files.createTempFile("feishu-file-", resolveFileSuffix(file.fileName(), file.mimeType()));
            try {
                Files.write(tempFile, file.data());
                CreateFileReq request = CreateFileReq.newBuilder()
                        .createFileReqBody(CreateFileReqBody.newBuilder()
                                .fileType(resolveFileType(file.mimeType()))
                                .fileName(defaultFileName(file.fileName(), file.mimeType()))
                                .file(tempFile.toFile())
                                .build())
                        .build();
                CreateFileResp response = apiClient.im().file().create(request);
                assertResponseSuccess(response.getCode(), response.getMsg());
                String fileKey = response.getData() != null ? response.getData().getFileKey() : null;
                if (fileKey == null || fileKey.isBlank()) {
                    throw new IllegalStateException("飞书文件上传成功但未返回 file_key");
                }
                return fileKey;
            } finally {
                Files.deleteIfExists(tempFile);
            }
        }

        private FeishuSendResult create(FeishuOutgoingMessage message) throws Exception {
            CreateMessageReq request = CreateMessageReq.newBuilder()
                    .receiveIdType(message.receiveIdType())
                    .createMessageReqBody(CreateMessageReqBody.newBuilder()
                            .receiveId(message.receiveId())
                            .msgType(message.msgType())
                            .content(message.content())
                            .uuid(message.uuid())
                            .build())
                    .build();
            CreateMessageResp response = apiClient.im().message().create(request);
            assertResponseSuccess(response.getCode(), response.getMsg());
            String remoteMessageId = response.getData() != null ? response.getData().getMessageId() : null;
            return new FeishuSendResult(FeishuSendRoute.CREATE, message.msgType(), remoteMessageId);
        }

        private FeishuSendResult reply(FeishuOutgoingMessage message) throws Exception {
            ReplyMessageReq request = ReplyMessageReq.newBuilder()
                    .messageId(message.targetMessageId())
                    .replyMessageReqBody(ReplyMessageReqBody.newBuilder()
                            .msgType(message.msgType())
                            .content(message.content())
                            .replyInThread(message.replyInThread())
                            .uuid(message.uuid())
                            .build())
                    .build();
            ReplyMessageResp response = apiClient.im().message().reply(request);
            assertResponseSuccess(response.getCode(), response.getMsg());
            String remoteMessageId = response.getData() != null ? response.getData().getMessageId() : null;
            return new FeishuSendResult(FeishuSendRoute.REPLY, message.msgType(), remoteMessageId);
        }

        private FeishuSendResult update(FeishuOutgoingMessage message) throws Exception {
            UpdateMessageReq request = UpdateMessageReq.newBuilder()
                    .messageId(message.targetMessageId())
                    .updateMessageReqBody(UpdateMessageReqBody.newBuilder()
                            .msgType(message.msgType())
                            .content(message.content())
                            .build())
                    .build();
            UpdateMessageResp response = apiClient.im().message().update(request);
            assertResponseSuccess(response.getCode(), response.getMsg());
            String remoteMessageId = response.getData() != null ? response.getData().getMessageId() : message.targetMessageId();
            return new FeishuSendResult(FeishuSendRoute.UPDATE, message.msgType(), remoteMessageId);
        }

        private FeishuSendResult patch(FeishuOutgoingMessage message) throws Exception {
            PatchMessageReq request = PatchMessageReq.newBuilder()
                    .messageId(message.targetMessageId())
                    .patchMessageReqBody(PatchMessageReqBody.newBuilder()
                            .content(message.content())
                            .build())
                    .build();
            PatchMessageResp response = apiClient.im().message().patch(request);
            assertResponseSuccess(response.getCode(), response.getMsg());
            return new FeishuSendResult(FeishuSendRoute.PATCH, message.msgType(), message.targetMessageId());
        }

        @Override
        public Map<String, Object> updateMessage(String messageId, String msgType, String content) throws Exception {
            UpdateMessageReq request = UpdateMessageReq.newBuilder()
                    .messageId(messageId)
                    .updateMessageReqBody(UpdateMessageReqBody.newBuilder()
                            .msgType(msgType)
                            .content(content)
                            .build())
                    .build();
            UpdateMessageResp response = apiClient.im().message().update(request);
            assertResponseSuccess(response.getCode(), response.getMsg());
            LinkedHashMap<String, Object> result = new LinkedHashMap<>();
            result.put("messageId", messageId);
            result.put("msgType", msgType);
            return Map.copyOf(result);
        }

        @Override
        public Map<String, Object> recallMessage(String messageId) throws Exception {
            DeleteMessageReq request = DeleteMessageReq.newBuilder()
                    .messageId(messageId)
                    .build();
            DeleteMessageResp response = apiClient.im().message().delete(request);
            assertResponseSuccess(response.getCode(), response.getMsg());
            return Map.of("messageId", messageId, "recalled", true);
        }

        @Override
        public Map<String, Object> downloadResource(String messageId, String fileKey, String type) throws Exception {
            GetMessageResourceReq request = GetMessageResourceReq.newBuilder()
                    .messageId(messageId)
                    .fileKey(fileKey)
                    .type(type)
                    .build();
            GetMessageResourceResp response = apiClient.im().messageResource().get(request);
            assertResponseSuccess(response.getCode(), response.getMsg());
            ByteArrayOutputStream data = response.getData();
            byte[] bytes = data != null ? data.toByteArray() : new byte[0];
            String base64Data = Base64.getEncoder().encodeToString(bytes);
            LinkedHashMap<String, Object> result = new LinkedHashMap<>();
            result.put("fileKey", fileKey);
            result.put("messageId", messageId);
            result.put("base64Data", base64Data);
            result.put("size", bytes.length);
            if (response.getFileName() != null && !response.getFileName().isBlank()) {
                result.put("fileName", response.getFileName());
            }
            return Map.copyOf(result);
        }

        @Override
        public Map<String, Object> createChat(String name, String description, String[] userIdList) throws Exception {
            CreateChatReqBody.Builder bodyBuilder = CreateChatReqBody.newBuilder()
                    .name(name);
            if (description != null && !description.isBlank()) {
                bodyBuilder.description(description);
            }
            if (userIdList != null && userIdList.length > 0) {
                bodyBuilder.userIdList(userIdList);
            }
            CreateChatReq request = CreateChatReq.newBuilder()
                    .createChatReqBody(bodyBuilder.build())
                    .build();
            CreateChatResp response = apiClient.im().chat().create(request);
            assertResponseSuccess(response.getCode(), response.getMsg());
            LinkedHashMap<String, Object> result = new LinkedHashMap<>();
            if (response.getData() != null) {
                result.put("chatId", response.getData().getChatId());
                result.put("name", response.getData().getName());
            }
            return Map.copyOf(result);
        }

        @Override
        public Map<String, Object> addChatMembers(String chatId, String memberIdType, String[] idList) throws Exception {
            CreateChatMembersReq request = CreateChatMembersReq.newBuilder()
                    .chatId(chatId)
                    .memberIdType(memberIdType != null ? memberIdType : "open_id")
                    .createChatMembersReqBody(CreateChatMembersReqBody.newBuilder()
                            .idList(idList)
                            .build())
                    .build();
            CreateChatMembersResp response = apiClient.im().chatMembers().create(request);
            assertResponseSuccess(response.getCode(), response.getMsg());
            return Map.of("chatId", chatId, "action", "add", "count", idList.length);
        }

        @Override
        public Map<String, Object> removeChatMembers(String chatId, String memberIdType, String[] idList) throws Exception {
            DeleteChatMembersReq request = DeleteChatMembersReq.newBuilder()
                    .chatId(chatId)
                    .memberIdType(memberIdType != null ? memberIdType : "open_id")
                    .deleteChatMembersReqBody(DeleteChatMembersReqBody.newBuilder()
                            .idList(idList)
                            .build())
                    .build();
            DeleteChatMembersResp response = apiClient.im().chatMembers().delete(request);
            assertResponseSuccess(response.getCode(), response.getMsg());
            return Map.of("chatId", chatId, "action", "remove", "count", idList.length);
        }

        @Override
        public Map<String, Object> createTask(String summary, @Nullable String dueTimestamp) throws Exception {
            InputTask.Builder taskBuilder = InputTask.newBuilder().summary(summary);
            if (dueTimestamp != null && !dueTimestamp.isBlank()) {
                taskBuilder.due(Due.newBuilder()
                        .timestamp(dueTimestamp.trim())
                        .isAllDay(false)
                        .build());
            }
            CreateTaskReq request = CreateTaskReq.newBuilder()
                    .inputTask(taskBuilder.build())
                    .build();
            CreateTaskResp response = apiClient.task().v2().task().create(request);
            assertResponseSuccess(response.getCode(), response.getMsg());
            LinkedHashMap<String, Object> result = new LinkedHashMap<>();
            if (response.getData() != null && response.getData().getTask() != null) {
                var task = response.getData().getTask();
                result.put("taskId", task.getGuid());
                if (task.getUrl() != null && !task.getUrl().isBlank()) {
                    result.put("url", task.getUrl());
                }
            }
            result.put("summary", summary);
            return Map.copyOf(result);
        }

        @Override
        public Map<String, Object> createDocument(String title, @Nullable String folderToken) throws Exception {
            CreateDocumentReqBody.Builder bodyBuilder = CreateDocumentReqBody.newBuilder().title(title);
            if (folderToken != null && !folderToken.isBlank()) {
                bodyBuilder.folderToken(folderToken.trim());
            }
            CreateDocumentReq request = CreateDocumentReq.newBuilder()
                    .createDocumentReqBody(bodyBuilder.build())
                    .build();
            CreateDocumentResp response = apiClient.docx().document().create(request);
            assertResponseSuccess(response.getCode(), response.getMsg());
            LinkedHashMap<String, Object> result = new LinkedHashMap<>();
            if (response.getData() != null && response.getData().getDocument() != null) {
                var doc = response.getData().getDocument();
                result.put("documentId", doc.getDocumentId());
                if (doc.getTitle() != null && !doc.getTitle().isBlank()) {
                    result.put("title", doc.getTitle());
                }
            }
            if (!result.containsKey("title")) {
                result.put("title", title);
            }
            return Map.copyOf(result);
        }

        @Override
        public Map<String, Object> createCalendarEvent(String calendarId, String summary,
                                                        String startTime, String endTime) throws Exception {
            CalendarEvent calendarEvent = CalendarEvent.newBuilder()
                    .summary(summary)
                    .startTime(TimeInfo.newBuilder().timestamp(startTime).build())
                    .endTime(TimeInfo.newBuilder().timestamp(endTime).build())
                    .build();
            CreateCalendarEventReq request = CreateCalendarEventReq.newBuilder()
                    .calendarId(calendarId)
                    .calendarEvent(calendarEvent)
                    .build();
            CreateCalendarEventResp response = apiClient.calendar().calendarEvent().create(request);
            assertResponseSuccess(response.getCode(), response.getMsg());
            LinkedHashMap<String, Object> result = new LinkedHashMap<>();
            if (response.getData() != null && response.getData().getEvent() != null) {
                var event = response.getData().getEvent();
                result.put("eventId", event.getEventId());
            }
            result.put("calendarId", calendarId);
            result.put("summary", summary);
            return Map.copyOf(result);
        }

        private void assertResponseSuccess(int code, @Nullable String message) {
            if (code != 0) {
                throw new IllegalStateException("飞书消息发送失败: code=" + code + ", message=" + message);
            }
        }

        private FeishuInboundMessage toInboundMessage(P2MessageReceiveV1 event) {
            P2MessageReceiveV1Data data = event.getEvent();
            UserId senderId = data != null && data.getSender() != null ? data.getSender().getSenderId() : null;
            String createTime = data != null && data.getMessage() != null ? data.getMessage().getCreateTime() : null;
            return new FeishuInboundMessage(
                    event.getHeader() != null ? event.getHeader().getEventId() : null,
                    data != null && data.getMessage() != null ? data.getMessage().getMessageId() : null,
                    data != null && data.getMessage() != null && data.getMessage().getMessageType() != null
                            ? data.getMessage().getMessageType()
                            : "text",
                    data != null && data.getMessage() != null ? data.getMessage().getChatId() : null,
                    data != null && data.getMessage() != null ? data.getMessage().getChatType() : null,
                    data != null && data.getMessage() != null ? data.getMessage().getThreadId() : null,
                    data != null && data.getMessage() != null ? data.getMessage().getRootId() : null,
                    data != null && data.getMessage() != null ? data.getMessage().getParentId() : null,
                    senderId != null ? senderId.getOpenId() : null,
                    senderId != null ? senderId.getUserId() : null,
                    senderId != null ? senderId.getUnionId() : null,
                    data != null && data.getSender() != null ? data.getSender().getSenderType() : null,
                    data != null && data.getSender() != null ? data.getSender().getTenantKey() : null,
                    data != null && data.getMessage() != null ? data.getMessage().getContent() : null,
                    parseInstant(createTime)
            );
        }

        @Nullable
        private Instant parseInstant(@Nullable String createTime) {
            if (createTime == null || createTime.isBlank()) {
                return null;
            }
            try {
                return Instant.ofEpochMilli(Long.parseLong(createTime));
            } catch (NumberFormatException ignored) {
                return null;
            }
        }

        private String blankToEmpty(@Nullable String value) {
            return value == null ? "" : value.trim();
        }

        private EventDispatcher buildEventDispatcher() {
            return EventDispatcher.newBuilder(
                            blankToEmpty(settings.verificationToken()),
                            blankToEmpty(settings.encryptKey())
                    )
                    .onP2CardActionTrigger(new P2CardActionTriggerHandler() {
                        @Override
                        public P2CardActionTriggerResponse handle(P2CardActionTrigger event) {
                            try {
                                log.info("收到卡片回调事件: eventId={}",
                                        event.getHeader() != null ? event.getHeader().getEventId() : "unknown");
                                FeishuInboundHandlingResult result = inboundHandler.apply(toCardActionInboundMessage(event));
                                log.info("卡片回调处理完成: type={}, content={}", result.toastType(), result.toastContent());
                                return buildCardActionResponse(result);
                            } catch (Exception e) {
                                log.error("卡片回调处理异常", e);
                                P2CardActionTriggerResponse response = new P2CardActionTriggerResponse();
                                CallBackToast toast = new CallBackToast();
                                toast.setType("error");
                                toast.setContent("处理失败");
                                response.setToast(toast);
                                return response;
                            }
                        }
                    })
                    .onP2MessageReceiveV1(new ImService.P2MessageReceiveV1Handler() {
                        @Override
                        public void handle(P2MessageReceiveV1 event) {
                            inboundHandler.apply(toInboundMessage(event));
                        }
                    })
                    .build();
        }

        private FeishuInboundMessage toCardActionInboundMessage(P2CardActionTrigger event) {
            P2CardActionTriggerData data = event.getEvent();
            CallBackOperator operator = data != null ? data.getOperator() : null;
            CallBackContext context = data != null ? data.getContext() : null;
            Header header = event.getHeader();

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("eventType", header != null ? header.getEventType() : "card.action.trigger");
            putIfNotBlank(payload, "token", data != null ? data.getToken() : null);
            putIfNotBlank(payload, "deliveryType", data != null ? data.getDeliveryType() : null);
            putIfNotBlank(payload, "host", data != null ? data.getHost() : null);
            payload.put("action", toActionPayload(data != null ? data.getAction() : null));
            payload.put("context", toContextPayload(context));
            payload.put("operator", toOperatorPayload(operator));

            return new FeishuInboundMessage(
                    header != null ? header.getEventId() : null,
                    context != null ? context.getOpenMessageId() : null,
                    "card-action",
                    context != null ? context.getOpenChatId() : null,
                    null,
                    null,
                    null,
                    null,
                    operator != null ? operator.getOpenId() : null,
                    operator != null ? operator.getUserId() : null,
                    operator != null ? operator.getUnionId() : null,
                    "user",
                    operator != null ? operator.getTenantKey() : event.getTenantKey(),
                    Jsons.DEFAULT.toJson(payload),
                    parseInstant(header != null ? header.getCreateTime() : null)
            );
        }

        private P2CardActionTriggerResponse buildCardActionResponse(FeishuInboundHandlingResult result) {
            P2CardActionTriggerResponse response = new P2CardActionTriggerResponse();
            CallBackToast toast = new CallBackToast();
            toast.setType(defaultToastType(result));
            toast.setContent(defaultToastContent(result));
            response.setToast(toast);
            return response;
        }

        private Map<String, Object> toActionPayload(@Nullable CallBackAction action) {
            if (action == null) {
                return Map.of();
            }
            LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
            putIfNotBlank(payload, "tag", action.getTag());
            putIfNotBlank(payload, "name", action.getName());
            putIfNotBlank(payload, "option", action.getOption());
            putIfNotBlank(payload, "timezone", action.getTimezone());
            putIfNotBlank(payload, "inputValue", action.getInputValue());
            if (action.getChecked() != null) {
                payload.put("checked", action.getChecked());
            }
            if (action.getOptions() != null && !action.getOptions().isEmpty()) {
                payload.put("options", List.copyOf(action.getOptions()));
            }
            if (action.getValue() != null && !action.getValue().isEmpty()) {
                payload.put("value", normalizeMap(action.getValue()));
                putIfNotBlank(payload, "label", asString(action.getValue().get("label")));
            }
            if (action.getFormValue() != null && !action.getFormValue().isEmpty()) {
                payload.put("formValue", normalizeMap(action.getFormValue()));
            }
            return Map.copyOf(payload);
        }

        private Map<String, Object> toContextPayload(@Nullable CallBackContext context) {
            if (context == null) {
                return Map.of();
            }
            LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
            putIfNotBlank(payload, "url", context.getUrl());
            putIfNotBlank(payload, "previewToken", context.getPreviewToken());
            putIfNotBlank(payload, "openMessageId", context.getOpenMessageId());
            putIfNotBlank(payload, "openChatId", context.getOpenChatId());
            return Map.copyOf(payload);
        }

        private Map<String, Object> toOperatorPayload(@Nullable CallBackOperator operator) {
            if (operator == null) {
                return Map.of();
            }
            LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
            putIfNotBlank(payload, "tenantKey", operator.getTenantKey());
            putIfNotBlank(payload, "userId", operator.getUserId());
            putIfNotBlank(payload, "openId", operator.getOpenId());
            putIfNotBlank(payload, "unionId", operator.getUnionId());
            return Map.copyOf(payload);
        }

        private Map<String, Object> normalizeMap(Map<String, Object> source) {
            LinkedHashMap<String, Object> normalized = new LinkedHashMap<>();
            source.forEach((key, value) -> normalized.put(key, normalizeValue(value)));
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
                LinkedHashMap<String, Object> normalized = new LinkedHashMap<>();
                map.forEach((key, nestedValue) -> {
                    if (key != null) {
                        normalized.put(String.valueOf(key), normalizeValue(nestedValue));
                    }
                });
                return Map.copyOf(normalized);
            }
            if (value instanceof List<?> list) {
                return normalizeList(list);
            }
            return value;
        }

        @Nullable
        private String asString(@Nullable Object value) {
            if (value instanceof String text && !text.isBlank()) {
                return text.trim();
            }
            return null;
        }

        private void putIfNotBlank(Map<String, Object> payload, String key, @Nullable String value) {
            if (value != null && !value.isBlank()) {
                payload.put(key, value.trim());
            }
        }

        private String resolveFileSuffix(FeishuImageUpload image) {
            String fileName = image.fileName();
            if (fileName != null) {
                int lastDot = fileName.lastIndexOf('.');
                if (lastDot >= 0 && lastDot < fileName.length() - 1) {
                    return fileName.substring(lastDot);
                }
            }
            String mimeType = image.mimeType();
            return guessSuffix(mimeType);
        }

        private String resolveFileSuffix(@Nullable String fileName, @Nullable String mimeType) {
            if (fileName != null) {
                int lastDot = fileName.lastIndexOf('.');
                if (lastDot >= 0 && lastDot < fileName.length() - 1) {
                    return fileName.substring(lastDot);
                }
            }
            return guessSuffix(mimeType);
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
                case "audio/mpeg" -> ".mp3";
                case "audio/wav" -> ".wav";
                case "video/mp4" -> ".mp4";
                case "application/pdf" -> ".pdf";
                default -> ".img";
            };
        }

        private String resolveFileType(@Nullable String mimeType) {
            if (mimeType == null) {
                return "stream";
            }
            String normalized = mimeType.toLowerCase();
            if (normalized.startsWith("video/")) {
                return "video";
            }
            if (normalized.startsWith("audio/")) {
                return "opus";
            }
            return "stream";
        }

        private String defaultFileName(@Nullable String fileName, @Nullable String mimeType) {
            if (fileName != null && !fileName.isBlank()) {
                return fileName.trim();
            }
            return "upload" + resolveFileSuffix(null, mimeType);
        }

        private String defaultToastType(FeishuInboundHandlingResult result) {
            if (result.toastType() != null && !result.toastType().isBlank()) {
                return result.toastType();
            }
            return result.accepted() ? "info" : "warning";
        }

        private String defaultToastContent(FeishuInboundHandlingResult result) {
            if (result.toastContent() != null && !result.toastContent().isBlank()) {
                return result.toastContent();
            }
            return result.accepted() ? "已收到操作，正在处理中" : "当前操作未被受理";
        }
    }
}
