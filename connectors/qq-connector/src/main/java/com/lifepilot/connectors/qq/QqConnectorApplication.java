package com.lifepilot.connectors.qq;

import com.lifepilot.connectors.qq.config.ConnectorRuntimeProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * QQ 机器人 connector 启动入口。
 *
 * @author zsg
 * @since 2026-04-03
 */
@SpringBootApplication
@ConfigurationPropertiesScan(basePackageClasses = ConnectorRuntimeProperties.class)
public class QqConnectorApplication {

    public static void main(String[] args) {
        SpringApplication.run(QqConnectorApplication.class, args);
    }
}
