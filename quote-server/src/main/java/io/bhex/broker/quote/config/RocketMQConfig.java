package io.bhex.broker.quote.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RocketMQConfig {
    private String namesrv;
    private String markPriceTopic;
}
