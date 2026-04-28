package com.lifepilot.connectors.feishu.web;

import com.lifepilot.connectors.feishu.model.ConnectorDeliveryRequest;
import com.lifepilot.connectors.feishu.model.ConnectorInstanceCommandRequest;
import com.lifepilot.connectors.feishu.model.ConnectorOperationRequest;
import com.lifepilot.connectors.feishu.model.ConnectorOperationResponse;
import com.lifepilot.connectors.feishu.service.FeishuConnectorService;
import com.lifepilot.connectors.feishu.service.FeishuWebhookRequest;
import com.lifepilot.connectors.feishu.service.FeishuWebhookResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 飞书 connector HTTP 入口。
 *
 * @author zsg
 * @since 2026-03-29
 */
@RestController
@RequestMapping("/instances")
public class FeishuConnectorController {

    private static final Logger log = LoggerFactory.getLogger(FeishuConnectorController.class);
    private static final String HEADER_INSTANCE_TOKEN = "X-Channel-Instance-Token";

    private final FeishuConnectorService feishuConnectorService;

    public FeishuConnectorController(FeishuConnectorService feishuConnectorService) {
        this.feishuConnectorService = feishuConnectorService;
    }

    @PostMapping("/{instanceId}/start")
    public ResponseEntity<?> start(@PathVariable String instanceId,
                                   @RequestHeader(name = HEADER_INSTANCE_TOKEN, required = false) String runtimeToken,
                                   @RequestBody ConnectorInstanceCommandRequest request) {
        Optional<ResponseEntity<?>> failure = authenticateForStart(instanceId, runtimeToken);
        if (failure.isPresent()) {
            return failure.get();
        }
        return ResponseEntity.ok(feishuConnectorService.start(instanceId, runtimeToken, request));
    }

    @PostMapping("/{instanceId}/stop")
    public ResponseEntity<?> stop(@PathVariable String instanceId,
                                  @RequestHeader(name = HEADER_INSTANCE_TOKEN, required = false) String runtimeToken) {
        Optional<ResponseEntity<?>> failure = authenticateExisting(instanceId, runtimeToken);
        if (failure.isPresent()) {
            return failure.get();
        }
        return ResponseEntity.ok(feishuConnectorService.stop(instanceId));
    }

    @PostMapping("/{instanceId}/reload")
    public ResponseEntity<?> reload(@PathVariable String instanceId,
                                    @RequestHeader(name = HEADER_INSTANCE_TOKEN, required = false) String runtimeToken,
                                    @RequestBody ConnectorInstanceCommandRequest request) {
        Optional<ResponseEntity<?>> failure = authenticateExisting(instanceId, runtimeToken);
        if (failure.isPresent()) {
            return failure.get();
        }
        return ResponseEntity.ok(feishuConnectorService.reload(instanceId, request));
    }

    @GetMapping("/{instanceId}/health")
    public ResponseEntity<?> health(@PathVariable String instanceId,
                                    @RequestHeader(name = HEADER_INSTANCE_TOKEN, required = false) String runtimeToken) {
        Optional<ResponseEntity<?>> failure = authenticateExisting(instanceId, runtimeToken);
        if (failure.isPresent()) {
            return failure.get();
        }
        return ResponseEntity.ok(feishuConnectorService.health(instanceId));
    }

    @PostMapping("/{instanceId}/webhook")
    public ResponseEntity<?> webhook(@PathVariable String instanceId,
                                     @RequestBody(required = false) byte[] body,
                                     HttpServletRequest request) {
        try {
            FeishuWebhookResponse response = feishuConnectorService.handleWebhook(
                    instanceId,
                    new FeishuWebhookRequest(
                            request.getRequestURI(),
                            body != null ? body : new byte[0],
                            extractHeaders(request)
                    )
            );
            HttpHeaders headers = new HttpHeaders();
            response.headers().forEach(headers::put);
            if (!headers.containsKey(HttpHeaders.CONTENT_TYPE)) {
                headers.setContentType(MediaType.APPLICATION_JSON);
            }
            return ResponseEntity.status(response.statusCode())
                    .headers(headers)
                    .body(response.body());
        } catch (SecurityException ex) {
            log.warn("飞书 webhook Verification Token 校验失败: instanceId={}", instanceId);
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(errorBody(ex.getMessage()));
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorBody(ex.getMessage()));
        } catch (IllegalStateException ex) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(errorBody(ex.getMessage()));
        } catch (Exception ex) {
            log.warn("飞书 webhook 处理失败: instanceId={}", instanceId, ex);
            return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(errorBody(ex.getMessage()));
        }
    }

    @PostMapping("/{instanceId}/deliver")
    public ResponseEntity<?> deliver(@PathVariable String instanceId,
                                     @RequestHeader(name = HEADER_INSTANCE_TOKEN, required = false) String runtimeToken,
                                     @RequestBody ConnectorDeliveryRequest request) {
        Optional<ResponseEntity<?>> failure = authenticateExisting(instanceId, runtimeToken);
        if (failure.isPresent()) {
            return failure.get();
        }
        try {
            // 流式 PATCH_STREAM 模式 — 入队等待 scheduler 异步 flush，立即 202 返回不阻塞主服务
            if ("PATCH_STREAM".equalsIgnoreCase(request.deliveryMode())) {
                feishuConnectorService.enqueueStreamingDelivery(instanceId, request);
                return ResponseEntity.accepted().build();
            }
            // 兜底：原 sync deliver 路径（覆盖 ASYNC_PUSH / 历史 null 调用）
            return ResponseEntity.ok(feishuConnectorService.deliver(instanceId, request));
        } catch (Exception ex) {
            log.warn("飞书主动消息发送失败: instanceId={}", instanceId, ex);
            return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(errorBody(ex.getMessage()));
        }
    }

    @PostMapping("/{instanceId}/operate")
    public ResponseEntity<?> operate(@PathVariable String instanceId,
                                     @RequestHeader(name = HEADER_INSTANCE_TOKEN, required = false) String runtimeToken,
                                     @RequestBody ConnectorOperationRequest request) {
        Optional<ResponseEntity<?>> failure = authenticateExisting(instanceId, runtimeToken);
        if (failure.isPresent()) {
            return failure.get();
        }
        ConnectorOperationResponse response = feishuConnectorService.handleOperation(instanceId, request);
        if (response.success()) {
            return ResponseEntity.ok(response);
        }
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(response);
    }

    @PostMapping("/{instanceId}/forward-heartbeat")
    public ResponseEntity<?> forwardHeartbeat(@PathVariable String instanceId,
                                              @RequestHeader(name = HEADER_INSTANCE_TOKEN, required = false) String runtimeToken) {
        Optional<ResponseEntity<?>> failure = authenticateExisting(instanceId, runtimeToken);
        if (failure.isPresent()) {
            return failure.get();
        }
        return ResponseEntity.ok(feishuConnectorService.forwardHeartbeat(instanceId));
    }

    private Optional<ResponseEntity<?>> authenticateForStart(String instanceId, String runtimeToken) {
        if (runtimeToken == null || runtimeToken.isBlank()) {
            return Optional.of(ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(errorBody("缺少实例令牌")));
        }
        if (feishuConnectorService.hasInstance(instanceId)
                && !feishuConnectorService.matchesToken(instanceId, runtimeToken)) {
            log.warn("飞书 connector 实例令牌校验失败: instanceId={}", instanceId);
            return Optional.of(ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(errorBody("实例令牌无效")));
        }
        return Optional.empty();
    }

    private Optional<ResponseEntity<?>> authenticateExisting(String instanceId, String runtimeToken) {
        if (runtimeToken == null || runtimeToken.isBlank()) {
            return Optional.of(ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(errorBody("缺少实例令牌")));
        }
        if (!feishuConnectorService.hasInstance(instanceId)) {
            return Optional.of(ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorBody("实例尚未启动")));
        }
        if (!feishuConnectorService.matchesToken(instanceId, runtimeToken)) {
            log.warn("飞书 connector 实例令牌校验失败: instanceId={}", instanceId);
            return Optional.of(ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(errorBody("实例令牌无效")));
        }
        return Optional.empty();
    }

    private Map<String, Object> errorBody(String message) {
        return Map.of("error", message);
    }

    private Map<String, List<String>> extractHeaders(HttpServletRequest request) {
        Map<String, List<String>> headers = new LinkedHashMap<>();
        for (String name : Collections.list(request.getHeaderNames())) {
            String normalized = normalizeHeaderName(name);
            headers.put(normalized, Collections.list(request.getHeaders(name)));
        }
        return Map.copyOf(headers);
    }

    private String normalizeHeaderName(String headerName) {
        if ("x-lark-request-timestamp".equalsIgnoreCase(headerName)) {
            return "X-Lark-Request-Timestamp";
        }
        if ("x-lark-request-nonce".equalsIgnoreCase(headerName)) {
            return "X-Lark-Request-Nonce";
        }
        if ("x-lark-signature".equalsIgnoreCase(headerName)) {
            return "X-Lark-Signature";
        }
        return headerName;
    }
}
