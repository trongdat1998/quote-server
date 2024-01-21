package io.bhex.broker.quote.data;

import io.bhex.base.quote.BookOrder;
import io.bhex.base.quote.Depth;
import io.bhex.base.quote.DepthList;
import io.bhex.broker.quote.data.dto.DepthDTO;
import io.bhex.broker.quote.data.dto.IEngineData;
import io.bhex.broker.quote.util.ConstantUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BookTicker implements IEngineData {
    private String symbol;
    private BigDecimal bidPrice;
    private BigDecimal bidQty;
    private BigDecimal askPrice;
    private BigDecimal askQty;
    private long time;

    public static BookTicker parse(Depth depth) {
        BookTickerBuilder builder = builder();
        DepthList asks = depth.getAsks();
        if (asks.getBookOrderCount() > 0) {
            BookOrder bookOrder = asks.getBookOrder(0);
            builder.askPrice(new BigDecimal(bookOrder.getPrice().getStr()));
            builder.askQty(new BigDecimal(bookOrder.getQuantity().getStr()));
        } else {
            builder.askPrice(BigDecimal.ZERO);
            builder.askPrice(BigDecimal.ZERO);
        }

        DepthList bids = depth.getBids();
        if (bids.getBookOrderCount() > 0) {
            BookOrder bookOrder = bids.getBookOrder(0);
            builder.bidPrice(new BigDecimal(bookOrder.getPrice().getStr()));
            builder.bidQty(new BigDecimal(bookOrder.getQuantity().getStr()));
        } else {
            builder.bidPrice(BigDecimal.ZERO);
            builder.bidQty(BigDecimal.ZERO);
        }
        builder.symbol(depth.getSymbol())
            .time(depth.getTime());
        return builder.build();
    }

    public static BookTicker parse(DepthDTO depthDTO) {
        BookTickerBuilder builder = builder();
        if (CollectionUtils.isNotEmpty(depthDTO.getBids()) && depthDTO.getBids().size() > 0) {
            builder.bidPrice(new BigDecimal(depthDTO.getBids().get(0)[0]));
            builder.bidQty(new BigDecimal(depthDTO.getBids().get(0)[1]));
        } else {
            builder.bidPrice(ConstantUtil.UNIQUE_ZERO);
            builder.bidQty(ConstantUtil.UNIQUE_ZERO);
        }

        if (CollectionUtils.isNotEmpty(depthDTO.getAsks()) && depthDTO.getAsks().size() > 0) {
            builder.askPrice(new BigDecimal(depthDTO.getAsks().get(0)[0]));
            builder.askQty(new BigDecimal(depthDTO.getAsks().get(0)[1]));
        } else {
            builder.askPrice(ConstantUtil.UNIQUE_ZERO);
            builder.askQty(ConstantUtil.UNIQUE_ZERO);
        }
        builder.symbol(depthDTO.getSymbol())
            .time(depthDTO.getTime());
        return builder.build();
    }
}

