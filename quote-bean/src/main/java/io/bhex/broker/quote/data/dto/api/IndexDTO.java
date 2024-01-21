package io.bhex.broker.quote.data.dto.api;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IndexDTO {

    private Map<String, BigDecimal> index;
    private Map<String, BigDecimal> edp;
}
