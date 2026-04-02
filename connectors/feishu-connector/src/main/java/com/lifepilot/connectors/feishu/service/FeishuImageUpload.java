package com.lifepilot.connectors.feishu.service;

import org.springframework.lang.Nullable;

/**
 * 飞书图片上传请求。
 *
 * @author zsg
 * @since 2026-03-29
 */
public record FeishuImageUpload(
        @Nullable String fileName,
        @Nullable String mimeType,
        byte[] data
) {

    public FeishuImageUpload {
        data = data != null ? data.clone() : new byte[0];
    }
}
