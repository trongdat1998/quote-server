package io.bhex.broker.quote.data.dto.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.Depth;
import io.bhex.base.quote.Realtime;
import io.bhex.broker.quote.data.dto.DepthDTO;
import io.bhex.broker.quote.data.dto.IEngineData;
import io.bhex.broker.quote.data.dto.RealTimeDTO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Objects;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TickerDTO implements IEngineData {
    /**
     * 时间
     */
    private long time;

    /**
     * 交易所
     */
    @JsonIgnore
    private long exchangeId;

    /**
     * 币对
     */
    private String symbol;

    /**
     * 收盘价
     */
    @JsonProperty("lastPrice")
    private BigDecimal close;

    /**
     * 最高价
     */
    @JsonProperty("highPrice")
    private BigDecimal high;

    /**
     * 最低价
     */
    @JsonProperty("lowPrice")
    private BigDecimal low;

    /**
     * 开盘价
     */
    @JsonProperty("openPrice")
    private BigDecimal open;

    private BigDecimal bestBidPrice;

    private BigDecimal bestAskPrice;

    /**
     * 成交量
     */
    private BigDecimal volume;

    /**
     * 成交额
     */
    private BigDecimal quoteVolume;

    /**
     * 开仓量，合约使用
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private BigDecimal openInterest;

    public static TickerDTO parse(Realtime realtime) {
        return TickerDTO.builder()
            .time(Long.parseLong(realtime.getT()))
            .exchangeId(realtime.getExchangeId())
            .symbol(realtime.getS())
            .close(new BigDecimal(realtime.getC()))
            .high(new BigDecimal(realtime.getH()))
            .low(new BigDecimal(realtime.getL()))
            .open(new BigDecimal(realtime.getO()))
            .volume(new BigDecimal(realtime.getV()))
            .quoteVolume(new BigDecimal(realtime.getQv()))
            .build();
    }

    public static TickerDTO parse(RealTimeDTO realTimeDTO) {
        return TickerDTO.builder()
            .time(realTimeDTO.getTime())
            .exchangeId(realTimeDTO.getExchangeId())
            .symbol(realTimeDTO.getSymbol())
            .close(realTimeDTO.getClose())
            .high(realTimeDTO.getHigh())
            .low(realTimeDTO.getLow())
            .open(realTimeDTO.getOpen())
            .volume(realTimeDTO.getVolume())
            .quoteVolume(realTimeDTO.getQuoteVolume())
            .openInterest(realTimeDTO.getOpenInterest())
            .build();
    }

    public static TickerDTO parse(Realtime realtime, Depth depth) {
        BigDecimal bestAskPrice = BigDecimal.ZERO;
        BigDecimal bestBidPrice = BigDecimal.ZERO;
        if (depth.getAsks().getBookOrderCount() != 0) {
            bestAskPrice = DecimalUtil.toBigDecimal(depth.getAsks().getBookOrder(0)
                .getPrice());
        }
        if (depth.getBids().getBookOrderCount() != 0) {
            bestBidPrice = DecimalUtil.toBigDecimal(depth.getBids().getBookOrder(0)
                .getPrice());
        }
        return TickerDTO.builder()
            .time(Long.parseLong(realtime.getT()))
            .exchangeId(realtime.getExchangeId())
            .symbol(realtime.getS())
            .close(new BigDecimal(realtime.getC()))
            .high(new BigDecimal(realtime.getH()))
            .low(new BigDecimal(realtime.getL()))
            .open(new BigDecimal(realtime.getO()))
            .volume(new BigDecimal(realtime.getV()))
            .quoteVolume(new BigDecimal(realtime.getQv()))
            .bestAskPrice(bestAskPrice)
            .bestBidPrice(bestBidPrice)
            .build();
    }

    public static TickerDTO parse(RealTimeDTO realTime, DepthDTO depth) {
        BigDecimal bestAskPrice = BigDecimal.ZERO;
        BigDecimal bestBidPrice = BigDecimal.ZERO;
        if (Objects.nonNull(depth.getAsks()) && depth.getAsks().size() != 0) {
            bestAskPrice = new BigDecimal(depth.getAsks().get(0)[0]);
        }
        if (Objects.nonNull(depth.getBids()) && depth.getBids().size() != 0) {
            bestBidPrice = new BigDecimal(depth.getBids().get(0)[0]);
        }
        return TickerDTO.builder()
            .time(realTime.getTime())
            .exchangeId(realTime.getExchangeId())
            .symbol(realTime.getSymbol())
            .close(realTime.getClose())
            .high(realTime.getHigh())
            .low(realTime.getLow())
            .open(realTime.getOpen())
            .volume(realTime.getVolume())
            .quoteVolume(realTime.getQuoteVolume())
            .bestAskPrice(bestAskPrice)
            .bestBidPrice(bestBidPrice)
            .openInterest(realTime.getOpenInterest())
            .build();
    }
}
