package io.bhex.broker.quote.client.task;

import io.bhex.ex.quote.core.enums.CommandType;
import lombok.Getter;

public abstract class AbstractSubscribeTask implements ISubscribeTask {
    @Getter
    protected Long exchangeId;
    @Getter
    protected String symbol;
    @Getter
    protected CommandType commandType;


    public AbstractSubscribeTask(Long exchangeId, String symbol, CommandType commandType) {
        this.exchangeId = exchangeId;
        this.symbol = symbol;
        this.commandType = commandType;
    }
}
