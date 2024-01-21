package io.bhex.broker.quote.repository;

import brave.internal.Nullable;
import io.bhex.base.quote.MarkPrice;
import io.bhex.broker.quote.data.dto.MarkPriceDTO;
import org.springframework.stereotype.Repository;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Repository
public class QuoteRepository {

    ConcurrentMap<Long, ConcurrentMap<String, MarkPrice>> exchangeSymbolMarkPriceMap = new ConcurrentHashMap<>();

    // 只有markPriceListener的线程写入
    public void updateMarkPrice(MarkPrice markPrice) {
        ConcurrentMap<String, MarkPrice> symbolMarkPriceMap = exchangeSymbolMarkPriceMap.get(markPrice.getExchangeId());
        if (Objects.isNull(symbolMarkPriceMap)) {
            symbolMarkPriceMap = new ConcurrentHashMap<>();
            exchangeSymbolMarkPriceMap.put(markPrice.getExchangeId(), symbolMarkPriceMap);
        }

        symbolMarkPriceMap.put(markPrice.getSymbolId(), markPrice);
    }

    @Nullable
    public MarkPrice getMarkPrice(Long exchangeId, String symbolId) {
        ConcurrentMap<String, MarkPrice> symbolMarkPriceMap = exchangeSymbolMarkPriceMap.get(exchangeId);
        if (Objects.nonNull(symbolMarkPriceMap)) {
            return symbolMarkPriceMap.get(symbolId);
        }
        return null;
    }

    @Nullable
    public MarkPriceDTO getMarkPriceDTO(Long exchangeId, String symbolId) {
        MarkPrice markPrice = getMarkPrice(exchangeId, symbolId);
        if (Objects.isNull(markPrice)) {
            return null;
        }

        return MarkPriceDTO.parseFrom(markPrice);
    }
}
