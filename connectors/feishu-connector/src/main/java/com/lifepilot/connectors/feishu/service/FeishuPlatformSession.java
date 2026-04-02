package com.lifepilot.connectors.feishu.service;

import org.springframework.lang.Nullable;

import java.util.Map;

/**
 * 飞书平台会话。
 *
 * @author zsg
 * @since 2026-03-29
 */
public interface FeishuPlatformSession {

    void start();

    void stop();

    FeishuSendResult send(FeishuOutgoingMessage message) throws Exception;

    default FeishuWebhookResponse handleWebhook(FeishuWebhookRequest request) throws Exception {
        throw new UnsupportedOperationException("当前飞书会话不支持 webhook 事件处理");
    }

    default String uploadImage(FeishuImageUpload image) throws Exception {
        throw new UnsupportedOperationException("当前飞书会话不支持图片上传");
    }

    default String uploadFile(FeishuFileUpload file) throws Exception {
        throw new UnsupportedOperationException("当前飞书会话不支持文件上传");
    }

    /**
     * 更新已发送的消息内容。
     */
    default Map<String, Object> updateMessage(String messageId, String msgType, String content) throws Exception {
        throw new UnsupportedOperationException("当前飞书会话不支持消息更新");
    }

    /**
     * 撤回消息。
     */
    default Map<String, Object> recallMessage(String messageId) throws Exception {
        throw new UnsupportedOperationException("当前飞书会话不支持消息撤回");
    }

    /**
     * 下载消息中的文件资源。
     */
    default Map<String, Object> downloadResource(String messageId, String fileKey, String type) throws Exception {
        throw new UnsupportedOperationException("当前飞书会话不支持文件下载");
    }

    /**
     * 创建群组。
     */
    default Map<String, Object> createChat(String name, String description, String[] userIdList) throws Exception {
        throw new UnsupportedOperationException("当前飞书会话不支持群组创建");
    }

    /**
     * 添加群成员。
     */
    default Map<String, Object> addChatMembers(String chatId, String memberIdType, String[] idList) throws Exception {
        throw new UnsupportedOperationException("当前飞书会话不支持群成员管理");
    }

    /**
     * 移除群成员。
     */
    default Map<String, Object> removeChatMembers(String chatId, String memberIdType, String[] idList) throws Exception {
        throw new UnsupportedOperationException("当前飞书会话不支持群成员管理");
    }

    /**
     * 创建飞书任务。
     *
     * @author zsg
     * @since 2026-04-02
     */
    default Map<String, Object> createTask(String summary, @Nullable String dueTimestamp) throws Exception {
        throw new UnsupportedOperationException("当前飞书会话不支持创建任务");
    }

    /**
     * 创建飞书文档。
     *
     * @author zsg
     * @since 2026-04-02
     */
    default Map<String, Object> createDocument(String title, @Nullable String folderToken) throws Exception {
        throw new UnsupportedOperationException("当前飞书会话不支持创建文档");
    }

    /**
     * 创建日程事件。
     *
     * @author zsg
     * @since 2026-04-02
     */
    default Map<String, Object> createCalendarEvent(String calendarId, String summary,
                                                     String startTime, String endTime) throws Exception {
        throw new UnsupportedOperationException("当前飞书会话不支持创建日程");
    }
}
