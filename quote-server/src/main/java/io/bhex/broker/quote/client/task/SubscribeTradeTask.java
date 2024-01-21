package io.bhex.broker.quote.client.task;

import io.bhex.base.match.Ticket;
import io.bhex.base.quote.QuoteRequest;
import io.bhex.broker.quote.client.QuoteEngineClient;
import io.bhex.broker.quote.client.QuoteInitializeManager;
import io.bhex.broker.quote.listener.TradeListener;
import io.bhex.broker.quote.repository.DataServiceRepository;
import io.bhex.broker.quote.util.BeanUtils;
import io.bhex.broker.quote.util.BrokerAuthUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.context.ApplicationContext;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.bhex.broker.quote.listener.TradeListener.TRADE_CACHED_SIZE;
import static io.bhex.ex.quote.core.enums.CommandType.TICKET;

@Slf4j
public class SubscribeTradeTask extends AbstractSubscribeTask {
    private ApplicationContext context;
    private DataServiceRepository dataServiceRepository;
    private QuoteInitializeManager quoteInitializeManager;
    private ScheduledExecutorService scheduledExecutorService;
    private QuoteEngineClient quoteEngineClient;

    public SubscribeTradeTask(Long exchangeId, String symbol, QuoteInitializeManager quoteManager,
                              ApplicationContext context, DataServiceRepository dataServiceRepository,
                              QuoteEngineClient quoteEngineClient) {
        super(exchangeId, symbol, TICKET);
        this.quoteInitializeManager = quoteManager;
        this.context = context;
        this.dataServiceRepository = dataServiceRepository;
        this.scheduledExecutorService = context.getBean(ScheduledExecutorService.class);
        this.quoteEngineClient = quoteEngineClient;
    }

    @Override
    public void run() {
        try {
            Long defaultOrgId = BrokerAuthUtil.getOrgIdSet().stream().findFirst().orElse(0L);
            List<Ticket> ticketList = dataServiceRepository.getTrades(exchangeId, symbol, 0,
                    TRADE_CACHED_SIZE, defaultOrgId);
            TradeListener tradeListener = context.getBean(BeanUtils.getTradesBeanName(exchangeId, symbol),
                TradeListener.class);
            tradeListener.clearSnapshot();
            if (CollectionUtils.isNotEmpty(ticketList)) {
                tradeListener.reloadHistoryData(ticketList);
            }

            quoteEngineClient.sendQuoteRequest(QuoteRequest.newBuilder()
                .setExchangeId(exchangeId)
                .setSymbol(symbol)
                .setSub(true)
                .setClientId(QuoteInitializeManager.getCID())
                .setTime(System.currentTimeMillis())
                .setType(commandType.getCode())
                .build());

            log.info("Subscribing [{}:{}:{}]", exchangeId, symbol, this.commandType);
        } catch (Exception e) {
            log.error("Resubscribe [{}:{}:{}] after 5s", exchangeId, symbol, commandType);
            log.error(e.getMessage(), e);
            scheduledExecutorService.schedule(this, 5, TimeUnit.SECONDS);
        }
    }
}
