package io.bhex.broker.quote.data.dto.v2;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.bhex.base.quote.Realtime;
import io.bhex.broker.quote.data.dto.IEngineData;
import io.bhex.broker.quote.data.dto.ISendData;
import io.bhex.broker.quote.util.ConstantUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * map.put("t", realtime.getT());
 * map.put("s", realtime.getS());
 * map.put("c", realtime.getC());
 * map.put("h", realtime.getH());
 * map.put("l", realtime.getL());
 * map.put("o", realtime.getO());
 * map.put("v", realtime.getV());
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RealTimeDTO implements ISendData, IEngineData {

    @JsonProperty("t")
    private String time;

    @JsonProperty("s")
    private String symbol;

    @JsonProperty("o")
    private BigDecimal open;

    @JsonProperty("h")
    private BigDecimal high;

    @JsonProperty("l")
    private BigDecimal low;

    @JsonProperty("c")
    private BigDecimal close;

    @JsonProperty("v")
    private BigDecimal volume;

    @JsonProperty("qv")
    private BigDecimal quoteVolume;

    @JsonProperty("m")
    private BigDecimal margin;

    public static RealTimeDTO parse(Realtime realtime) {
        BigDecimal open = new BigDecimal(realtime.getO());
        BigDecimal close = new BigDecimal(realtime.getC());
        BigDecimal margin = ConstantUtil.UNIQUE_ZERO.compareTo(open) == 0 ?
            ConstantUtil.UNIQUE_ZERO : close.subtract(open).divide(open, 4, RoundingMode.HALF_UP);
        return RealTimeDTO.builder()
            .symbol(realtime.getS())
            .time(realtime.getT())
            .close(close)
            .high(new BigDecimal(realtime.getH()))
            .low(new BigDecimal(realtime.getL()))
            .open(open)
            .volume(new BigDecimal(realtime.getV()))
            .quoteVolume(new BigDecimal(realtime.getQv()))
            .margin(margin)
            .build();
    }

    public static RealTimeDTO getDefaultRealTime(Long exchangeId, String symbol) {
        return RealTimeDTO.builder()
            .symbol(symbol)
            .time(System.currentTimeMillis() + "")
            .low(ConstantUtil.UNIQUE_ZERO)
            .high(ConstantUtil.UNIQUE_ZERO)
            .open(ConstantUtil.UNIQUE_ZERO)
            .close(ConstantUtil.UNIQUE_ZERO)
            .volume(ConstantUtil.UNIQUE_ZERO)
            .quoteVolume(ConstantUtil.UNIQUE_ZERO)
            .margin(ConstantUtil.UNIQUE_ZERO)
            .build();
    }

    @Override
    @JsonIgnore
    public long getCurId() {
        return Long.valueOf(this.time);
    }

    public long getTime() {
        return Long.valueOf(this.time);
    }
}
