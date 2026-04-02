package com.lifepilot.connectors.feishu.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * connector 运行时配置。
 *
 * @author zsg
 * @since 2026-03-29
 */
@ConfigurationProperties(prefix = "connector.runtime")
public class ConnectorRuntimeProperties {

    private String zhiweiBaseUrl = "http://127.0.0.1:8080";
    private Duration imageDownloadTimeout = Duration.ofSeconds(15);
    private long imageMaxBytes = 10 * 1024 * 1024;

    public String getZhiweiBaseUrl() {
        return zhiweiBaseUrl;
    }

    public void setZhiweiBaseUrl(String zhiweiBaseUrl) {
        this.zhiweiBaseUrl = zhiweiBaseUrl;
    }

    public Duration getImageDownloadTimeout() {
        return imageDownloadTimeout;
    }

    public void setImageDownloadTimeout(Duration imageDownloadTimeout) {
        this.imageDownloadTimeout = imageDownloadTimeout;
    }

    public long getImageMaxBytes() {
        return imageMaxBytes;
    }

    public void setImageMaxBytes(long imageMaxBytes) {
        this.imageMaxBytes = imageMaxBytes;
    }
}
