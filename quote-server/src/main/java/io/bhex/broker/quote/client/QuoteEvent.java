package io.bhex.broker.quote.client;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class QuoteEvent<T> {
    QuoteStreamIndex quoteStreamIndex;
    T data;
    QuoteEventTypeEnum type;
}
