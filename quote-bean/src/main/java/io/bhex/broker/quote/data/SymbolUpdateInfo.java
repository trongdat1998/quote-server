package io.bhex.broker.quote.data;

import io.bhex.broker.grpc.basic.Symbol;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SymbolUpdateInfo {

    public Set<Symbol> newSymbolSet;
    public Map<Long, Set<String>> pendingRemovedSymbolMap;
}
