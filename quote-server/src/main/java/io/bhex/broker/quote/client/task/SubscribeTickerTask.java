package io.bhex.broker.quote.client.task;

import io.bhex.base.quote.QuoteRequest;
import io.bhex.base.quote.Realtime;
import io.bhex.broker.quote.client.QuoteEngineClient;
import io.bhex.broker.quote.client.QuoteInitializeManager;
import io.bhex.broker.quote.enums.RealtimeIntervalEnum;
import io.bhex.broker.quote.listener.BrokerListener;
import io.bhex.broker.quote.listener.RealTimeListener;
import io.bhex.broker.quote.listener.SlowBrokerListener;
import io.bhex.broker.quote.listener.TopNListener;
import io.bhex.broker.quote.repository.DataServiceRepository;
import io.bhex.broker.quote.repository.SymbolRepository;
import io.bhex.broker.quote.util.BeanUtils;
import io.bhex.broker.quote.util.BrokerAuthUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.bhex.broker.quote.listener.TopNListener.ALL;
import static io.bhex.ex.quote.core.enums.CommandType.REAL_TIME;

@Slf4j
public class SubscribeTickerTask extends AbstractSubscribeTask {
    private ApplicationContext context;
    private DataServiceRepository dataServiceRepository;
    private QuoteInitializeManager quoteInitializeManager;
    private ScheduledExecutorService scheduledExecutorService;
    private SymbolRepository symbolRepository;
    private QuoteEngineClient quoteEngineClient;

    public SubscribeTickerTask(Long exchangeId, String symbol, QuoteInitializeManager quoteManager,
                               ApplicationContext context, DataServiceRepository dataServiceRepository,
                               QuoteEngineClient quoteEngineClient) {
        super(exchangeId, symbol, REAL_TIME);
        this.quoteInitializeManager = quoteManager;
        this.context = context;
        this.dataServiceRepository = dataServiceRepository;
        this.symbolRepository = context.getBean(SymbolRepository.class);
        this.scheduledExecutorService = context.getBean(ScheduledExecutorService.class);
        this.quoteEngineClient = quoteEngineClient;
    }

    @Override
    public void run() {
        try {
            //临时填充orgId
            Long defaultOrgId = BrokerAuthUtil.getOrgIdSet().stream().findFirst().orElse(0L);
            Realtime realtime = dataServiceRepository.getRealTime(exchangeId, symbol, defaultOrgId);

            if (Objects.nonNull(realtime)) {
                RealTimeListener realTimeListener = context.getBean(BeanUtils.getRealTimeBeanName(exchangeId, symbol, RealtimeIntervalEnum.H24.getInterval()),
                    RealTimeListener.class);
                realTimeListener.onEngineMessage(realtime);
                Set<Long> orgIds = symbolRepository.getOrgIdsByExchangeIdAndSymbol(realtime.getExchangeId(), realtime.getS());
                for (Long orgId : orgIds) {
                    int curType = symbolRepository.getSymbolType(symbol);
                    try {
                        TopNListener topNListener = context.getBean(BeanUtils.getBrokerTopNBeanName(orgId,
                            curType, RealtimeIntervalEnum.H24.getInterval()),
                            TopNListener.class);
                        topNListener.onEngineMessage(realtime);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }

                    try {
                        TopNListener topNListener = context.getBean(BeanUtils.getBrokerTopNBeanName(orgId,
                            ALL, RealtimeIntervalEnum.H24.getInterval()), TopNListener.class);
                        topNListener.onEngineMessage(realtime);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }

                    try {
                        BrokerListener brokerListener = context.getBean(BeanUtils.getBrokerBeanName(orgId,
                            curType, RealtimeIntervalEnum.H24.getInterval()),
                            BrokerListener.class);
                        brokerListener.onEngineMessage(realtime);
                        SlowBrokerListener slowBrokerListener = context.getBean(BeanUtils.getSlowBrokerBeanName(orgId,
                            curType, RealtimeIntervalEnum.H24.getInterval()), SlowBrokerListener.class);
                        slowBrokerListener.onEngineMessage(realtime);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }

                    try {
                        BrokerListener brokerListener = context.getBean(BeanUtils.getBrokerBeanName(orgId,
                            ALL, RealtimeIntervalEnum.H24.getInterval()),
                            BrokerListener.class);
                        brokerListener.onEngineMessage(realtime);
                        SlowBrokerListener slowBrokerListener = context.getBean(BeanUtils.getSlowBrokerBeanName(orgId,
                            ALL, RealtimeIntervalEnum.H24.getInterval()), SlowBrokerListener.class);
                        slowBrokerListener.onEngineMessage(realtime);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }

            quoteEngineClient.sendQuoteRequest(QuoteRequest.newBuilder()
                .setTime(System.currentTimeMillis())
                .setClientId(QuoteInitializeManager.getCID())
                .setType(REAL_TIME.getCode())
                .setExchangeId(exchangeId)
                .setSymbol(symbol)
                .setSub(true)
                .build());

            log.info("Subscribing [{}:{}:{}]", exchangeId, symbol, this.commandType);
        } catch (Exception e) {
            log.error("Resubscribe [{}:{}:{}] after 5s", exchangeId, symbol, commandType);
            log.error(e.getMessage(), e);
            scheduledExecutorService.schedule(this, 5, TimeUnit.SECONDS);
        }
    }
}
