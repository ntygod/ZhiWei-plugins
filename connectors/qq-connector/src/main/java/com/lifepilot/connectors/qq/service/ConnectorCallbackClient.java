package com.lifepilot.connectors.qq.service;

import com.lifepilot.connectors.qq.model.ConnectorEventRequest;
import com.lifepilot.connectors.qq.model.ConnectorEventResponse;

import java.util.Map;

/**
 * 知微主服务回调客户端。
 *
 * @author zsg
 * @since 2026-04-03
 */
public interface ConnectorCallbackClient {

    ConnectorEventResponse submitEvent(String instanceId, String runtimeToken, ConnectorEventRequest request);

    Map<String, Object> forwardHeartbeat(String instanceId, String runtimeToken);
}
