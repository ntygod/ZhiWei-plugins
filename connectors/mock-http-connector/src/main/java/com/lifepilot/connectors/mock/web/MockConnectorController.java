package com.lifepilot.connectors.mock.web;

import com.lifepilot.connectors.mock.model.ConnectorDeliveryRequest;
import com.lifepilot.connectors.mock.model.ConnectorEventResponse;
import com.lifepilot.connectors.mock.model.ConnectorInstanceCommandRequest;
import com.lifepilot.connectors.mock.model.SimulatedInboundEventRequest;
import com.lifepilot.connectors.mock.service.MockConnectorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Optional;

/**
 * mock connector HTTP 入口。
 *
 * @author zsg
 * @since 2026-03-29
 */
@RestController
@RequestMapping("/instances")
public class MockConnectorController {

    private static final Logger log = LoggerFactory.getLogger(MockConnectorController.class);
    private static final String HEADER_INSTANCE_TOKEN = "X-Channel-Instance-Token";

    private final MockConnectorService mockConnectorService;

    public MockConnectorController(MockConnectorService mockConnectorService) {
        this.mockConnectorService = mockConnectorService;
    }

    @PostMapping("/{instanceId}/start")
    public ResponseEntity<?> start(@PathVariable String instanceId,
                                   @RequestHeader(name = HEADER_INSTANCE_TOKEN, required = false) String runtimeToken,
                                   @RequestBody ConnectorInstanceCommandRequest request) {
        Optional<ResponseEntity<?>> failure = authenticateForStart(instanceId, runtimeToken);
        if (failure.isPresent()) {
            return failure.get();
        }
        return ResponseEntity.ok(mockConnectorService.start(instanceId, runtimeToken, request));
    }

    @PostMapping("/{instanceId}/stop")
    public ResponseEntity<?> stop(@PathVariable String instanceId,
                                  @RequestHeader(name = HEADER_INSTANCE_TOKEN, required = false) String runtimeToken) {
        Optional<ResponseEntity<?>> failure = authenticateExisting(instanceId, runtimeToken);
        if (failure.isPresent()) {
            return failure.get();
        }
        return ResponseEntity.ok(mockConnectorService.stop(instanceId));
    }

    @PostMapping("/{instanceId}/reload")
    public ResponseEntity<?> reload(@PathVariable String instanceId,
                                    @RequestHeader(name = HEADER_INSTANCE_TOKEN, required = false) String runtimeToken,
                                    @RequestBody ConnectorInstanceCommandRequest request) {
        Optional<ResponseEntity<?>> failure = authenticateExisting(instanceId, runtimeToken);
        if (failure.isPresent()) {
            return failure.get();
        }
        return ResponseEntity.ok(mockConnectorService.reload(instanceId, request));
    }

    @GetMapping("/{instanceId}/health")
    public ResponseEntity<?> health(@PathVariable String instanceId,
                                    @RequestHeader(name = HEADER_INSTANCE_TOKEN, required = false) String runtimeToken) {
        Optional<ResponseEntity<?>> failure = authenticateExisting(instanceId, runtimeToken);
        if (failure.isPresent()) {
            return failure.get();
        }
        return ResponseEntity.ok(mockConnectorService.health(instanceId));
    }

    @PostMapping("/{instanceId}/deliver")
    public ResponseEntity<?> deliver(@PathVariable String instanceId,
                                     @RequestHeader(name = HEADER_INSTANCE_TOKEN, required = false) String runtimeToken,
                                     @RequestBody ConnectorDeliveryRequest request) {
        Optional<ResponseEntity<?>> failure = authenticateExisting(instanceId, runtimeToken);
        if (failure.isPresent()) {
            return failure.get();
        }
        return ResponseEntity.ok(mockConnectorService.deliver(instanceId, request));
    }

    @PostMapping("/{instanceId}/simulate-event")
    public ResponseEntity<?> simulateEvent(@PathVariable String instanceId,
                                           @RequestHeader(name = HEADER_INSTANCE_TOKEN, required = false) String runtimeToken,
                                           @RequestBody SimulatedInboundEventRequest request) {
        Optional<ResponseEntity<?>> failure = authenticateExisting(instanceId, runtimeToken);
        if (failure.isPresent()) {
            return failure.get();
        }
        ConnectorEventResponse response = mockConnectorService.simulateEvent(instanceId, request);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/{instanceId}/forward-heartbeat")
    public ResponseEntity<?> forwardHeartbeat(@PathVariable String instanceId,
                                              @RequestHeader(name = HEADER_INSTANCE_TOKEN, required = false) String runtimeToken) {
        Optional<ResponseEntity<?>> failure = authenticateExisting(instanceId, runtimeToken);
        if (failure.isPresent()) {
            return failure.get();
        }
        return ResponseEntity.ok(mockConnectorService.forwardHeartbeat(instanceId));
    }

    private Optional<ResponseEntity<?>> authenticateForStart(String instanceId, String runtimeToken) {
        if (runtimeToken == null || runtimeToken.isBlank()) {
            return Optional.of(ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(errorBody("缺少实例令牌")));
        }
        if (mockConnectorService.hasInstance(instanceId) && !mockConnectorService.matchesToken(instanceId, runtimeToken)) {
            log.warn("mock connector 实例令牌校验失败: instanceId={}", instanceId);
            return Optional.of(ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(errorBody("实例令牌无效")));
        }
        return Optional.empty();
    }

    private Optional<ResponseEntity<?>> authenticateExisting(String instanceId, String runtimeToken) {
        if (runtimeToken == null || runtimeToken.isBlank()) {
            return Optional.of(ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(errorBody("缺少实例令牌")));
        }
        if (!mockConnectorService.hasInstance(instanceId)) {
            return Optional.of(ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorBody("实例尚未启动")));
        }
        if (!mockConnectorService.matchesToken(instanceId, runtimeToken)) {
            log.warn("mock connector 实例令牌校验失败: instanceId={}", instanceId);
            return Optional.of(ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(errorBody("实例令牌无效")));
        }
        return Optional.empty();
    }

    private Map<String, Object> errorBody(String message) {
        return Map.of("error", message);
    }
}
