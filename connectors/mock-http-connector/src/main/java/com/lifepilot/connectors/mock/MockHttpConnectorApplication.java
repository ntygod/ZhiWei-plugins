package com.lifepilot.connectors.mock;

import com.lifepilot.connectors.mock.config.ConnectorRuntimeProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Mock HTTP connector 启动入口。
 *
 * @author zsg
 * @since 2026-03-29
 */
@SpringBootApplication
@ConfigurationPropertiesScan(basePackageClasses = ConnectorRuntimeProperties.class)
public class MockHttpConnectorApplication {

    public static void main(String[] args) {
        SpringApplication.run(MockHttpConnectorApplication.class, args);
    }
}
