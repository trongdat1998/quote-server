package io.bhex.broker.quote.data.dto.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.bhex.broker.quote.data.dto.IEngineData;
import io.bhex.broker.quote.data.dto.ISymbol;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DepthDTO implements IEngineData, ISymbol {

    @JsonIgnore
    public static final int MAX_LIMIT = 100;

    private long time;

    @JsonIgnore
    private String symbol;

    /**
     * 买入深度
     * 高到低
     */
    private List<BigDecimal[]> bids;

    /**
     * 卖出深度
     * 低到高
     */
    private List<BigDecimal[]> asks;

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
    public static DepthDTO parse(io.bhex.broker.quote.data.dto.DepthDTO depthDTO) {
        DepthDTO.DepthDTOBuilder builder = builder()
            .time(depthDTO.getTime());

        List<String[]> asks = depthDTO.getAsks();
        if (CollectionUtils.isNotEmpty(asks)) {
            builder.asks(transform(asks));
        } else {
            builder.asks(Collections.emptyList());
        }

        List<String[]> bids = depthDTO.getBids();
        if (CollectionUtils.isNotEmpty(bids)) {
            builder.bids(transform(bids));
        } else {
            builder.bids(Collections.emptyList());
        }

        return builder.build();
    }

    @JsonIgnore
    private static List<BigDecimal[]> transform(List<String[]> childDepth) {
        List<BigDecimal[]> bigDecimals = new ArrayList<>();
        for (String[] ask : childDepth) {
            BigDecimal[] askDecimal = new BigDecimal[ask.length];
            askDecimal[0] = new BigDecimal(ask[0]);
            askDecimal[1] = new BigDecimal(ask[1]);
            bigDecimals.add(askDecimal);
        }
        return bigDecimals;
    }
}
