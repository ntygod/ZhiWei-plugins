package com.lifepilot.connectors.feishu;

import com.lifepilot.connectors.feishu.config.ConnectorRuntimeProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

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
}
