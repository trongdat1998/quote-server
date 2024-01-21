package io.bhex.broker.quote.listener.v2;

public interface IExchangeSymbolListener<T> extends ITopicListener<T> {
    Long getExchangeId();

    String getSymbolId();
}
