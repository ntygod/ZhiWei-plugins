package com.lifepilot.connectors.qq.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * connector 运行时配置。
 *
 * @author zsg
 * @since 2026-04-03
 */
@ConfigurationProperties(prefix = "connector.runtime")
public class ConnectorRuntimeProperties {

    private String zhiweiBaseUrl = "http://127.0.0.1:8080";

    public String getZhiweiBaseUrl() {
        return zhiweiBaseUrl;
    }

    public void setZhiweiBaseUrl(String zhiweiBaseUrl) {
        this.zhiweiBaseUrl = zhiweiBaseUrl;
    }
}
