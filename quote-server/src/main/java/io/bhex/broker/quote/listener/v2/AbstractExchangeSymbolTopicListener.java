package io.bhex.broker.quote.listener.v2;

import io.bhex.broker.quote.data.dto.IEngineData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;

@Slf4j
public abstract class AbstractExchangeSymbolTopicListener<T, DTO extends IEngineData>
    extends AbstractTopicListener<T, DTO>
    implements IExchangeSymbolListener<T> {
    private Long exchangeId;
    private String symbolId;

    public AbstractExchangeSymbolTopicListener(Long exchangeId, String symbolId, ApplicationContext context) {
        this(context);
        this.exchangeId = exchangeId;
        this.symbolId = symbolId;
    }

    public AbstractExchangeSymbolTopicListener(ApplicationContext context) {
        super(context);
    }

    @Override
    public Long getExchangeId() {
        return exchangeId;
    }

    @Override
    public String getSymbolId() {
        return symbolId;
    }

    public void onError(Throwable e) {
        log.info("[{}:{}:{}] error ", exchangeId, symbolId, getTopic().name());
        super.onError(e);
    }

}
