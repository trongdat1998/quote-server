package io.bhex.broker.quote.data.dto.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.bhex.base.quote.Realtime;
import io.bhex.broker.quote.data.dto.RealTimeDTO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TickerPriceDTO {
    @JsonIgnore
    private long exchangeId;
    private String symbol;
    private BigDecimal price;

    public static TickerPriceDTO parse(Realtime realtime) {
        return TickerPriceDTO.builder()
            .exchangeId(realtime.getExchangeId())
            .symbol(realtime.getS())
            .price(new BigDecimal(realtime.getC()))
            .build();
    }

    public static TickerPriceDTO parse(RealTimeDTO realTimeDTO) {
        return TickerPriceDTO.builder()
            .exchangeId(realTimeDTO.getExchangeId())
            .symbol(realTimeDTO.getSymbol())
            .price(realTimeDTO.getClose())
            .build();
    }
}
