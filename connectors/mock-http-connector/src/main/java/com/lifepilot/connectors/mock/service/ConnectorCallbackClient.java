package com.lifepilot.connectors.mock.service;

import com.lifepilot.connectors.mock.model.ConnectorEventRequest;
import com.lifepilot.connectors.mock.model.ConnectorEventResponse;

import java.util.Map;

/**
 * 知微主服务回调客户端。
 *
 * @author zsg
 * @since 2026-03-29
 */
public interface ConnectorCallbackClient {

    ConnectorEventResponse submitEvent(String instanceId, String runtimeToken, ConnectorEventRequest request);

    Map<String, Object> forwardHeartbeat(String instanceId, String runtimeToken);
}
