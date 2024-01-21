package io.bhex.broker.quote.data.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.KLine;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * map.put("t", kLine.getId());
 * map.put("s", kLine.getSymbol());
 * map.put("c", DecimalUtil.toBigDecimal(kLine.getClose()).toPlainString());
 * map.put("h", DecimalUtil.toBigDecimal(kLine.getHigh()).toPlainString());
 * map.put("l", DecimalUtil.toBigDecimal(kLine.getLow()).toPlainString());
 * map.put("o", DecimalUtil.toBigDecimal(kLine.getOpen()).toPlainString());
 * map.put("v", DecimalUtil.toBigDecimal(kLine.getVolume()).toPlainString());
 */
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Data
public class KlineItemDTO implements ISendData, IEngineData {
    @JsonIgnore
    public static final int MAX_LIMIT = 2000;
    @JsonIgnore
    public static final int MAX_MULTI_LIMIT = 200;

    @JsonProperty("t")
    private long time;

    @JsonProperty("s")
    private String symbol;

    @JsonProperty("sn")
    private String symbolName;

    @JsonProperty("c")
    private BigDecimal close;

    @JsonProperty("h")
    private BigDecimal high;

    @JsonProperty("l")
    private BigDecimal low;

    @JsonProperty("o")
    private BigDecimal open;

    @JsonProperty("v")
    private BigDecimal volume;

    public static KlineItemDTO parseIndexKline(KLine kLine) {
        return KlineItemDTO.builder()
            .time(kLine.getId())
            .symbol(kLine.getSymbol())
            .symbolName(kLine.getSymbol())
            .close(DecimalUtil.toBigDecimal(kLine.getClose()))
            .high(DecimalUtil.toBigDecimal(kLine.getHigh()))
            .low(DecimalUtil.toBigDecimal(kLine.getLow()))
            .open(DecimalUtil.toBigDecimal(kLine.getOpen()))
            .volume(DecimalUtil.toBigDecimal(kLine.getVolume()))
            .build();
    }

    public static KlineItemDTO parse(KLine kLine, String symbolName) {
        return KlineItemDTO.builder()
            .time(kLine.getId())
            .symbol(kLine.getSymbol())
            .symbolName(symbolName)
            .close(DecimalUtil.toBigDecimal(kLine.getClose()))
            .high(DecimalUtil.toBigDecimal(kLine.getHigh()))
            .low(DecimalUtil.toBigDecimal(kLine.getLow()))
            .open(DecimalUtil.toBigDecimal(kLine.getOpen()))
            .volume(DecimalUtil.toBigDecimal(kLine.getVolume()))
            .build();
    }

    public void cloneData(KlineItemDTO itemDTO) {
        this.time = itemDTO.getTime();
        this.symbol = itemDTO.getSymbol();
        this.close = itemDTO.getClose();
        this.high = itemDTO.getHigh();
        this.low = itemDTO.getLow();
        this.open = itemDTO.getOpen();
        this.volume = itemDTO.getVolume();
        this.symbolName = itemDTO.getSymbolName();
    }

    @Override
    @JsonIgnore
    public long getCurId() {
        return this.time;
    }
}
