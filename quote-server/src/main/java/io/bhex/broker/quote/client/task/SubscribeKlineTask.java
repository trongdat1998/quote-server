package io.bhex.broker.quote.client.task;

import io.bhex.base.quote.KLine;
import io.bhex.base.quote.QuoteRequest;
import io.bhex.broker.quote.client.QuoteEngineClient;
import io.bhex.broker.quote.client.QuoteInitializeManager;
import io.bhex.broker.quote.listener.KlineListener;
import io.bhex.broker.quote.repository.DataServiceRepository;
import io.bhex.broker.quote.util.BeanUtils;
import io.bhex.broker.quote.util.BrokerAuthUtil;
import io.bhex.broker.quote.client.DayRealtimeKline;
import io.bhex.exchange.enums.KlineIntervalEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.context.ApplicationContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.bhex.broker.quote.listener.KlineListener.KLINE_CACHE_SIZE;
import static io.bhex.ex.quote.core.enums.CommandType.KLINE;

@Slf4j
public class SubscribeKlineTask extends AbstractSubscribeTask {
    private ApplicationContext context;
    private DataServiceRepository dataServiceRepository;
    private QuoteInitializeManager quoteInitializeManager;
    private ScheduledExecutorService scheduledExecutorService;
    private QuoteEngineClient quoteEngineClient;
    private DayRealtimeKline dayRealtimeKline;
    public SubscribeKlineTask(Long exchangeId, String symbol, QuoteInitializeManager quoteManager,
                              ApplicationContext context, DataServiceRepository dataServiceRepository,
                              QuoteEngineClient quoteEngineClient) {
        super(exchangeId, symbol, KLINE);
        this.quoteInitializeManager = quoteManager;
        this.context = context;
        this.dataServiceRepository = dataServiceRepository;
        this.scheduledExecutorService = context.getBean(ScheduledExecutorService.class);
        this.quoteEngineClient = quoteEngineClient;
        this.dayRealtimeKline = context.getBean(DayRealtimeKline.class);
    }

    @Override
    public void run() {
        try {
            Long defaultOrgId = BrokerAuthUtil.getOrgIdSet().stream().findFirst().orElse(0L);
            Map<KlineIntervalEnum, List<KLine>> cacheListMap = new HashMap<>();
            for (KlineIntervalEnum value : KlineIntervalEnum.values()) {
                List<KLine> kLineList =
                        dataServiceRepository.getKlineList(exchangeId, symbol, value.interval, KLINE_CACHE_SIZE, defaultOrgId);
                cacheListMap.put(value, kLineList);
            }

            for (Map.Entry<KlineIntervalEnum, List<KLine>> klineIntervalEnumListEntry : cacheListMap.entrySet()) {
                KlineIntervalEnum intervalEnum = klineIntervalEnumListEntry.getKey();
                List<KLine> kLineList = klineIntervalEnumListEntry.getValue();
                KlineListener klineListener = context.getBean(BeanUtils.getKlineBeanName(exchangeId, symbol,
                    intervalEnum.interval), KlineListener.class);
                klineListener.clearSnapshot();
                if (CollectionUtils.isNotEmpty(kLineList)) {
                    klineListener.reloadHistoryData(kLineList);
                    //处理日线相关的数据
                    if (KlineIntervalEnum.D1.equals(intervalEnum) || KlineIntervalEnum.D1_8.equals(intervalEnum)) {
                        dayRealtimeKline.handleDayKline(kLineList.get(kLineList.size() - 1), intervalEnum.interval);
                    }
                }
            }

            quoteEngineClient.sendQuoteRequest(QuoteRequest.newBuilder()
                .setClientId(QuoteInitializeManager.getCID())
                .setSub(true)
                .setType(commandType.getCode())
                .setExchangeId(exchangeId)
                .setSymbol(symbol)
                .setTime(System.currentTimeMillis())
                .build());

            log.info("Subscribing [{}:{}:{}]", exchangeId, symbol, this.commandType);
        } catch (Exception e) {
            log.error("Resubscribe [{}:{}:{}] after 5s", exchangeId, symbol, commandType);
            log.error(e.getMessage(), e);
            scheduledExecutorService.schedule(this, 5, TimeUnit.SECONDS);
        }
    }
}
