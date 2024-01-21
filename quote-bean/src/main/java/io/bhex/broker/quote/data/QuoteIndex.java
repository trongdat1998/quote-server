package io.bhex.broker.quote.data;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@Builder
public class QuoteIndex {

    private Long exchangeId;
    private String symbol;

    @NoArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class KlineIndex extends QuoteIndex {
        private String klineType;
        private long from;
        private long to;

        @Builder
        public KlineIndex(Long exchangeId, String symbol, String klineType, long from, long to) {
            super(exchangeId, symbol);
            this.klineType = klineType;
            this.from = from;
            this.to = to;
        }

        public static class KlineIndexBuilder extends QuoteIndexBuilder {
            KlineIndexBuilder() {
                super();
            }
        }
    }
}
