package io.bhex.broker.quote.data;

import io.bhex.broker.grpc.basic.Symbol;
import io.bhex.broker.grpc.broker.Broker;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BrokerSymbolUpdateInfo {
    private List<Broker> newBrokerList;
    private List<Broker> pendingRemovedBrokerList;
    private Map<Long, Map<QuoteIndex, Symbol>> newBrokerSymbolSetMap;
    private Map<Long, Map<QuoteIndex, Symbol>> pendingRemovedBrokerSymbolSetMap;

}
