package io.bhex.broker.quote.data.dto;

import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.MarkPrice;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MarkPriceDTO implements IEngineData {
    private Long exchangeId;
    private String symbolId;
    private BigDecimal price;
    private Long time;

    @Override
    public long getTime() {
        return this.time;
    }

    public static MarkPriceDTO parseFrom(MarkPrice markPrice) {
        return MarkPriceDTO.builder()
            .exchangeId(markPrice.getExchangeId())
            .symbolId(markPrice.getSymbolId())
            .price(DecimalUtil.toBigDecimal(markPrice.getMarkPrice()))
            .time(markPrice.getTime())
            .build();
    }
}
