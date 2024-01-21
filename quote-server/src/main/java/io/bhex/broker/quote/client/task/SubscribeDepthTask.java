package io.bhex.broker.quote.client.task;

import io.bhex.base.quote.Depth;
import io.bhex.base.quote.QuoteRequest;
import io.bhex.broker.quote.client.QuoteEngineClient;
import io.bhex.broker.quote.client.QuoteInitializeManager;
import io.bhex.broker.quote.listener.DepthListener;
import io.bhex.broker.quote.listener.DiffDepthListener;
import io.bhex.broker.quote.listener.DiffMergedDepthListener;
import io.bhex.broker.quote.listener.MergedDepthListener;
import io.bhex.broker.quote.repository.DataServiceRepository;
import io.bhex.broker.quote.repository.SymbolRepository;
import io.bhex.broker.quote.util.BeanUtils;
import io.bhex.broker.quote.util.BrokerAuthUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;

import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.bhex.broker.quote.util.ConstantUtil.DEFAULT_DEPTH_LIMIT;
import static io.bhex.ex.quote.core.enums.CommandType.DEPTH;

@Slf4j
public class SubscribeDepthTask extends AbstractSubscribeTask {
    private ApplicationContext context;
    private DataServiceRepository dataServiceRepository;
    private QuoteInitializeManager quoteInitializeManager;
    private SymbolRepository symbolRepository;
    private ScheduledExecutorService scheduledExecutorService;
    private QuoteEngineClient quoteEngineClient;

    public SubscribeDepthTask(Long exchangeId, String symbol, QuoteInitializeManager quoteManager,
                              ApplicationContext context, DataServiceRepository dataServiceRepository,
                              SymbolRepository symbolRepository,
                              QuoteEngineClient quoteEngineClient) {
        super(exchangeId, symbol, DEPTH);
        this.quoteInitializeManager = quoteManager;
        this.context = context;
        this.dataServiceRepository = dataServiceRepository;
        this.symbolRepository = symbolRepository;
        this.scheduledExecutorService = context.getBean(ScheduledExecutorService.class);
        this.quoteEngineClient = quoteEngineClient;
    }

    @Override
    public void run() {
        try {
            Long defaultOrgId = BrokerAuthUtil.getOrgIdSet().stream().findFirst().orElse(0L);
            Depth depth = dataServiceRepository.getPartialDepth(exchangeId, symbol, DEFAULT_DEPTH_LIMIT, defaultOrgId);

            DepthListener depthListener = context.getBean(BeanUtils.getDepthBeanName(exchangeId, symbol),
                DepthListener.class);
            DiffDepthListener diffDepthListener = context.getBean(BeanUtils.getDiffDepthBeanName(exchangeId, symbol),
                DiffDepthListener.class);

            if (Objects.nonNull(depth)) {
                depthListener.onEngineMessage(depth);
                diffDepthListener.onEngineMessage(depth);
            }

            // 合并精度的深度
            int[] mergedArray = symbolRepository.getMergedArray(symbol);
            for (int dumpScale : mergedArray) {

                Depth mergedDepth = dataServiceRepository.getPartialDepth(exchangeId, symbol, dumpScale,
                        DEFAULT_DEPTH_LIMIT, defaultOrgId);

                MergedDepthListener mergedDepthListener = context.getBean(BeanUtils
                    .getDepthBeanName(exchangeId, symbol, dumpScale), MergedDepthListener.class);
                DiffMergedDepthListener diffMergedDepthListener = context.getBean(BeanUtils
                    .getDiffDepthBeanName(exchangeId, symbol, dumpScale), DiffMergedDepthListener.class);

                mergedDepthListener.clearSnapshot();
                diffMergedDepthListener.clearSnapshot();

                if (Objects.nonNull(mergedDepth)) {
                    mergedDepthListener.onEngineMessage(mergedDepth);
                    diffMergedDepthListener.onEngineMessage(mergedDepth);
                }
            }

            quoteEngineClient.sendQuoteRequest(QuoteRequest.newBuilder()
                .setClientId(QuoteInitializeManager.getCID())
                .setExchangeId(exchangeId)
                .setSymbol(symbol)
                .setType(commandType.getCode())
                .setTime(System.currentTimeMillis())
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
