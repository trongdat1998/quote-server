package io.bhex.broker.quote.data.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.Depth;
import io.bhex.base.quote.DepthList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Map<String, Object> map = new LinkedHashMap<>();
 * map.put("t", depth.getTime());
 * map.put("v", depth.getVersion());
 * map.put("s", depth.getSymbol().toUpperCase());
 * <p>
 * List<String[]> b = getArrayList(depth.getBids());
 * List<String[]> a = getArrayList(depth.getAsks());
 * <p>
 * map.put("b", b);
 * map.put("a", a);
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DepthDTO implements ISendData, IEngineData {

    @JsonIgnore
    public static final int MAX_LIMIT = 100;

    @JsonProperty("e")
    private long exchangeId;

    @JsonProperty("s")
    private String symbol;

    @JsonProperty("sn")
    private String symbolName;

    @JsonProperty("t")
    private Long time;

    @JsonProperty("v")
    private String version;

    @JsonProperty("b")
    private List<String[]> bids;

    @JsonProperty("a")
    private List<String[]> asks;

    @JsonProperty("o")
    private long order;

    public DepthDTO(DepthDTO depthDTO) {
        this.exchangeId = depthDTO.exchangeId;
        this.symbol = depthDTO.symbol;
        this.symbolName = depthDTO.symbolName;
        this.time = depthDTO.time;
        this.version = depthDTO.version;
        this.bids = depthDTO.bids;
        this.asks = depthDTO.asks;
        this.order = depthDTO.order;
    }

    @JsonIgnore
    public void truncate(int limit) {
        if (bids.size() > limit) {
            bids = bids.subList(0, limit);
        }
        if (asks.size() > limit) {
            asks = asks.subList(0, limit);
        }
    }

    @JsonIgnore
    private static List<String[]> getArrayList(DepthList list) {
        return list.getBookOrderList()
            .stream()
            .map(bookOrder -> new String[]{
                DecimalUtil.toBigDecimal(bookOrder.getPrice()).stripTrailingZeros().toPlainString(),
                DecimalUtil.toBigDecimal(bookOrder.getQuantity()).stripTrailingZeros().toPlainString(),
            })
            .collect(Collectors.toList());
    }

    @JsonIgnore
    public static DepthDTO parse(Depth depth, String symbolName) {
        return DepthDTO.builder()
            .symbol(depth.getSymbol())
            .asks(getArrayList(depth.getAsks()))
            .bids(getArrayList(depth.getBids()))
            .time(depth.getTime())
            .version(depth.getVersion())
            .exchangeId(depth.getExchangeId())
            .build();
    }

    @JsonIgnore
    public static DepthDTO getDefaultDepthDTO(Long exchangeId, String symbol) {
        return DepthDTO.builder()
            .exchangeId(exchangeId)
            .symbol(symbol)
            .symbolName(symbol)
            .time(System.currentTimeMillis())
            .asks(Collections.emptyList())
            .bids(Collections.emptyList())
            .build();
    }

    @Override
    @JsonIgnore
    public long getCurId() {
        return this.time;
    }

    @Override
    public long getTime() {
        return time;
    }

}
