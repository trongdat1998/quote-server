package io.bhex.broker.quote.client.task;

import java.util.concurrent.Callable;

public interface ISubscribeTask extends Runnable {
    Long getExchangeId();
    String getSymbol();
}
