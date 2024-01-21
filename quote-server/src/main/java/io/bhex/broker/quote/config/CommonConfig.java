package io.bhex.broker.quote.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@ConfigurationProperties(prefix = "bhex.quote")
@Component
public class CommonConfig {
    private boolean online = true;
}
