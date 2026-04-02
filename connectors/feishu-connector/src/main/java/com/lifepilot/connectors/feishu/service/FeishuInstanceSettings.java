package com.lifepilot.connectors.feishu.service;

import com.lifepilot.connectors.feishu.model.ConnectorInstanceCommandRequest;
import org.springframework.lang.Nullable;

import java.util.Map;

/**
 * 飞书实例配置快照。
 *
 * @author zsg
 * @since 2026-03-29
 */
public record FeishuInstanceSettings(
        String instanceId,
        String pluginId,
        String platform,
        String appId,
        String appSecret,
        String connectionMode,
        @Nullable String verificationToken,
        @Nullable String encryptKey,
        Map<String, Object> config,
        Map<String, Object> secretConfig,
        Map<String, Object> routingPolicy
) {

    public static FeishuInstanceSettings from(ConnectorInstanceCommandRequest request) {
        Map<String, Object> config = request.config() != null ? request.config() : Map.of();
        Map<String, Object> secretConfig = request.secretConfig() != null ? request.secretConfig() : Map.of();
        Map<String, Object> routingPolicy = request.routingPolicy() != null ? request.routingPolicy() : Map.of();

        String appId = firstNonBlank(secretConfig, "appId", config, "appId");
        String appSecret = firstNonBlank(secretConfig, "appSecret", config, "appSecret");
        String connectionMode = optionalString(config, "connectionMode", "websocket");
        String verificationToken = firstNonBlankOrNull(secretConfig, "verificationToken", config, "verificationToken");
        String encryptKey = firstNonBlankOrNull(secretConfig, "encryptKey", config, "encryptKey");

        if (appId == null || appId.isBlank()) {
            throw new IllegalArgumentException("缺少飞书 App ID");
        }
        if (appSecret == null || appSecret.isBlank()) {
            throw new IllegalArgumentException("缺少飞书 App Secret");
        }
        if (!"websocket".equalsIgnoreCase(connectionMode) && !"webhook".equalsIgnoreCase(connectionMode)) {
            throw new IllegalArgumentException("飞书 connectionMode 仅支持 websocket 或 webhook");
        }
        if ("webhook".equalsIgnoreCase(connectionMode)) {
            if (verificationToken == null || verificationToken.isBlank()) {
                throw new IllegalArgumentException("webhook 模式缺少 Verification Token");
            }
            if (encryptKey == null || encryptKey.isBlank()) {
                throw new IllegalArgumentException("webhook 模式缺少 Encrypt Key");
            }
        }

        return new FeishuInstanceSettings(
                request.instanceId(),
                request.pluginId(),
                request.platform(),
                appId.trim(),
                appSecret.trim(),
                connectionMode.trim(),
                verificationToken,
                encryptKey,
                Map.copyOf(config),
                Map.copyOf(secretConfig),
                Map.copyOf(routingPolicy)
        );
    }

    @Nullable
    private static String firstNonBlankOrNull(Map<String, Object> primary,
                                              String primaryKey,
                                              Map<String, Object> secondary,
                                              String secondaryKey) {
        String value = firstNonBlank(primary, primaryKey, secondary, secondaryKey);
        return value != null && !value.isBlank() ? value.trim() : null;
    }

    @Nullable
    private static String firstNonBlank(Map<String, Object> primary,
                                        String primaryKey,
                                        Map<String, Object> secondary,
                                        String secondaryKey) {
        String value = optionalString(primary, primaryKey, null);
        if (value != null && !value.isBlank()) {
            return value;
        }
        return optionalString(secondary, secondaryKey, null);
    }

    @Nullable
    private static String optionalString(Map<String, Object> source,
                                         String key,
                                         @Nullable String defaultValue) {
        Object raw = source.get(key);
        if (raw instanceof String value && !value.isBlank()) {
            return value.trim();
        }
        return defaultValue;
    }
}
