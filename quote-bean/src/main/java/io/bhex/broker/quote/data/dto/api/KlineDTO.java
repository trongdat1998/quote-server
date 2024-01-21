package io.bhex.broker.quote.data.dto.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import io.bhex.base.proto.Decimal;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.KLine;
import io.bhex.broker.quote.data.dto.IEngineData;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KlineDTO implements IEngineData {
    @JsonIgnore
    public static final int MAX_LIMIT = 1000;

    /**
     * [
     * 1499040000000,      // Open time
     * "0.01634790",       // Open
     * "0.80000000",       // High
     * "0.01575800",       // Low
     * "0.01577100",       // Close
     * "148976.11427815",  // Volume
     * 1499644799999,      // Close time
     * "2434.19055334",    // Quote asset volume
     * 308,                // Number of trades
     * "1756.87402397",    // Taker buy base asset volume
     * "28.46694368",      // Taker buy quote asset volume
     * ]
     */
    @JsonValue
    private List<Object> klineItem;

    public static KlineDTO parse(KLine kLine) {
        List<Object> klineItem = new ArrayList<>();
        klineItem.add(kLine.getId());
        klineItem.add(DecimalUtil.toBigDecimal(kLine.getOpen()));
        klineItem.add(DecimalUtil.toBigDecimal(kLine.getHigh()));
        klineItem.add(DecimalUtil.toBigDecimal(kLine.getLow()));
        klineItem.add(DecimalUtil.toBigDecimal(kLine.getClose()));
        klineItem.add(DecimalUtil.toBigDecimal(kLine.getVolume()));
        klineItem.add(kLine.getEndTime());
        klineItem.add(DecimalUtil.toBigDecimal(kLine.getQuoteVolume()));
        klineItem.add(kLine.getTrades());
        klineItem.add(DecimalUtil.toBigDecimal(kLine.getTakerBaseVolume()));
        klineItem.add(DecimalUtil.toBigDecimal(kLine.getTakerQuoteVolume()));

        return KlineDTO.builder()
            .klineItem(klineItem)
            .build();

    }

    @Override
    public long getTime() {
        return (long) klineItem.get(0);
    }
}
