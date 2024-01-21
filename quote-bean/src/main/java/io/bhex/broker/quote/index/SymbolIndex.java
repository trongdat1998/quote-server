package io.bhex.broker.quote.index;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Util for connect symbol with exchange and orgId
 */
@Data
@AllArgsConstructor
@Builder
public class SymbolIndex {
    private String symbol;
    private ConcurrentMap<Long, CopyOnWriteArraySet<Long>> orgIdExchangeIdMap;
    public SymbolIndex() {
        orgIdExchangeIdMap = new ConcurrentHashMap<>();
    }

    public void addExchangeId(Long exchangeId, Long orgId) {
        CopyOnWriteArraySet<Long> exchangeIdSet = orgIdExchangeIdMap.get(orgId);
        if (Objects.isNull(exchangeIdSet)) {
            exchangeIdSet = new CopyOnWriteArraySet<>();
            orgIdExchangeIdMap.put(orgId, exchangeIdSet);
        }
        exchangeIdSet.add(exchangeId);
    }

    public Long getExchangeId(Long orgId) {
        Set<Long> exchangeIdSet = orgIdExchangeIdMap.get(orgId);
        if (CollectionUtils.isEmpty(exchangeIdSet)) {
            return null;
        }
        return exchangeIdSet.iterator().next();
    }
}
