package io.bhex.broker.quote.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties("bhex.broker")
@Data
@Component
@NoArgsConstructor
@AllArgsConstructor
public class BrokerServerConfig {
    private String name;
    private String host;
    private int port;

    @JsonIgnore
    public String getBrokerGrpcServerUrl() {
        return "grpc://" + host + ":" + port;
    }
}
