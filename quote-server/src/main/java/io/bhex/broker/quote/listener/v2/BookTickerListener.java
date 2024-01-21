package io.bhex.broker.quote.listener.v2;

import io.bhex.base.quote.Depth;
import io.bhex.broker.quote.data.BookTicker;
import io.bhex.broker.quote.enums.TopicEnum;
import org.springframework.context.ApplicationContext;

public class BookTickerListener extends AbstractExchangeSymbolTopicListener<Depth, BookTicker> {
    public BookTickerListener(Long exchangeId, String symbolId, ApplicationContext context) {
        super(exchangeId, symbolId, context);
    }

    @Override
    protected BookTicker convertFrom(Depth data) {
        return BookTicker.parse(data);
    }

    @Override
    public TopicEnum getTopic() {
        return TopicEnum.bookTicker;
    }
}
