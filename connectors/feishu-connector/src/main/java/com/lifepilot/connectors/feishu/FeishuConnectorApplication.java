package com.lifepilot.connectors.feishu;

import com.lifepilot.connectors.feishu.config.ConnectorRuntimeProperties;
import com.lifepilot.connectors.feishu.service.FeishuConnectorService;
import com.lifepilot.connectors.feishu.streaming.FeishuFlushAction;
import com.lifepilot.connectors.feishu.streaming.StreamingDeliveryFlushScheduler;
import com.lifepilot.connectors.feishu.streaming.StreamingDeliveryQueue;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;

/**
 * 飞书 connector 启动入口。
 *
 * @author zsg
 * @since 2026-03-29
 */
@SpringBootApplication
@ConfigurationPropertiesScan(basePackageClasses = ConnectorRuntimeProperties.class)
public class FeishuConnectorApplication {

    public static void main(String[] args) {
        SpringApplication.run(FeishuConnectorApplication.class, args);
    }

    /**
     * 流式投递队列 — 主服务 PATCH_STREAM 请求入队后由 scheduler 节流落地。
     */
    @Bean
    public StreamingDeliveryQueue streamingDeliveryQueue() {
        return new StreamingDeliveryQueue();
    }

    /**
     * 流式投递 flush 调度器 — Spring 启动时 start，关闭时自动 stop（daemon 线程不阻塞退出）。
     *
     * <p>注入顺序：queue + service + properties → action 桥接 → scheduler。</p>
     */
    @Bean(initMethod = "start", destroyMethod = "stop")
    public StreamingDeliveryFlushScheduler streamingDeliveryFlushScheduler(
            StreamingDeliveryQueue queue,
            ConnectorRuntimeProperties properties,
            FeishuConnectorService service) {
        return new StreamingDeliveryFlushScheduler(
                queue,
                properties.getStreaming().toPolicy(),
                new FeishuFlushAction(service)
        );
    }
}
