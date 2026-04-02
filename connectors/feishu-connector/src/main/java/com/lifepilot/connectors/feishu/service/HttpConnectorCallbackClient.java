package com.lifepilot.connectors.feishu.service;

import com.lifepilot.connectors.feishu.config.ConnectorRuntimeProperties;
import com.lifepilot.connectors.feishu.model.ConnectorEventRequest;
import com.lifepilot.connectors.feishu.model.ConnectorEventResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.Map;

/**
 * 基于 HTTP 的知微回调客户端。
 *
 * @author zsg
 * @since 2026-03-29
 */
@Component
public class HttpConnectorCallbackClient implements ConnectorCallbackClient {

    private static final String HEADER_INSTANCE_TOKEN = "X-Channel-Instance-Token";

    private final RestClient restClient;
    private final ConnectorRuntimeProperties properties;

    public HttpConnectorCallbackClient(RestClient.Builder restClientBuilder,
                                       ConnectorRuntimeProperties properties) {
        this.restClient = restClientBuilder.build();
        this.properties = properties;
    }

    @Override
    public ConnectorEventResponse submitEvent(String instanceId,
                                              String runtimeToken,
                                              ConnectorEventRequest request) {
        return restClient.post()
                .uri(runtimeBaseUrl() + "/api/channel-runtime/instances/" + instanceId + "/events")
                .header(HEADER_INSTANCE_TOKEN, runtimeToken)
                .body(request)
                .retrieve()
                .body(ConnectorEventResponse.class);
    }

    @Override
    public Map<String, Object> forwardHeartbeat(String instanceId, String runtimeToken) {
        return restClient.post()
                .uri(runtimeBaseUrl() + "/api/channel-runtime/instances/" + instanceId + "/heartbeat")
                .header(HEADER_INSTANCE_TOKEN, runtimeToken)
                .retrieve()
                .body(Map.class);
    }

    private String runtimeBaseUrl() {
        String value = properties.getZhiweiBaseUrl();
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }
}
