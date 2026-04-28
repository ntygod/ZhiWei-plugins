package com.lifepilot.connectors.feishu.config;

import com.lifepilot.connectors.feishu.streaming.FeishuStreamingPolicy;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;

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
    private StreamingProperties streaming = new StreamingProperties();

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

    public StreamingProperties getStreaming() {
        return streaming;
    }

    public void setStreaming(StreamingProperties streaming) {
        this.streaming = streaming;
    }

    /** 流式投递节流配置（仅 PATCH_STREAM 模式生效）。 */
    public static class StreamingProperties {
        private Duration firstByteWindow = Duration.ofSeconds(5);
        private Duration firstByteInterval = Duration.ofMillis(200);
        private Duration steadyInterval = Duration.ofMillis(1500);
        private List<Duration> backoffStages = List.of(
                Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(4));
        private Duration idleThreshold = Duration.ofSeconds(30);

        public Duration getFirstByteWindow() {
            return firstByteWindow;
        }

        public void setFirstByteWindow(Duration firstByteWindow) {
            this.firstByteWindow = firstByteWindow;
        }

        public Duration getFirstByteInterval() {
            return firstByteInterval;
        }

        public void setFirstByteInterval(Duration firstByteInterval) {
            this.firstByteInterval = firstByteInterval;
        }

        public Duration getSteadyInterval() {
            return steadyInterval;
        }

        public void setSteadyInterval(Duration steadyInterval) {
            this.steadyInterval = steadyInterval;
        }

        public List<Duration> getBackoffStages() {
            return backoffStages;
        }

        public void setBackoffStages(List<Duration> backoffStages) {
            this.backoffStages = backoffStages;
        }

        public Duration getIdleThreshold() {
            return idleThreshold;
        }

        public void setIdleThreshold(Duration idleThreshold) {
            this.idleThreshold = idleThreshold;
        }

        public FeishuStreamingPolicy toPolicy() {
            return new FeishuStreamingPolicy(
                    firstByteWindow, firstByteInterval, steadyInterval, backoffStages, idleThreshold);
        }
    }
}
