package io.bhex.broker.quote.data;

import io.bhex.base.quote.Depth;
import io.bhex.base.quote.Realtime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Builder
@Data
public class BookTickerMessage {
    private Depth depth;
    private Realtime realtime;
}
