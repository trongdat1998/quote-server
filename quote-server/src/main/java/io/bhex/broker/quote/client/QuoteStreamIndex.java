package io.bhex.broker.quote.client;

import io.bhex.ex.quote.core.enums.CommandType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@EqualsAndHashCode
public class QuoteStreamIndex {
    private Long exchangeId;
    private String symbol;
    private CommandType commandType;
}
