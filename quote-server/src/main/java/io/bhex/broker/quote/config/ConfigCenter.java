package io.bhex.broker.quote.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ConfigurationProperties(prefix = "bhex")
@Component
@Data
public class ConfigCenter {

    /**
     * 1. 两个都为空时，服务获取的所有的exchangeId
     * 2. 两个都有值时，优先检查in-service-exchanges
     * 3. 只有out-of-service-brokers时，过滤不服务的broker
     * 4. 只有in-service-brokers，过滤服务的broker
     */
    private List<Long> inServiceBrokers;
    private List<Long> outOfServiceBrokers;

    private Map<String, String> partitionMap;

    private String platform;
}
