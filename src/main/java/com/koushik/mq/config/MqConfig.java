package com.koushik.mq.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Broker configuration driven by environment variables.
 */
@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "mq")
public class MqConfig {

    private int brokerId = 1;
    private String dataDir = "./data";
    private int defaultPartitionCount = 3;
    private int replicationFactor = 1;
    private String peers = "";
}
