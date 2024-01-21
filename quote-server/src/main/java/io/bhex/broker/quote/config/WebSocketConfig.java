package io.bhex.broker.quote.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@ConfigurationProperties("bhex.ws")
@Component
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class WebSocketConfig {
    private int port;
}
