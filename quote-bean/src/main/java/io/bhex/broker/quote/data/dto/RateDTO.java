package io.bhex.broker.quote.data.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Map;

@AllArgsConstructor
@Builder
@Data
@NoArgsConstructor
public class RateDTO {
    private String token;
    private Map<String, BigDecimal> rates;
}
