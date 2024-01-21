package io.bhex.broker.quote.controller;

import io.bhex.broker.grpc.basic.Symbol;
import io.bhex.broker.quote.client.QuoteInitializeManager;
import io.bhex.broker.quote.config.CommonConfig;
import io.bhex.broker.quote.enums.RealtimeIntervalEnum;
import io.bhex.broker.quote.job.BrokerInfoTask;
import io.bhex.broker.quote.listener.BrokerListener;
import io.bhex.broker.quote.listener.SlowBrokerListener;
import io.bhex.broker.quote.listener.TopNListener;
import io.bhex.broker.quote.util.BeanUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashSet;
import java.util.Set;

@RestController
@RequestMapping("/admin")
@Slf4j
public class AdminController {

    private final CommonConfig commonConfig;
    private final QuoteInitializeManager quoteInitializeManager;
    private final ApplicationContext context;

    @Autowired
    public AdminController(CommonConfig commonConfig,
                           QuoteInitializeManager quoteInitializeManager,
                           ApplicationContext context) {
        this.commonConfig = commonConfig;
        this.quoteInitializeManager = quoteInitializeManager;
        this.context = context;
    }

    // 从topN listener 和 broker listener中移除 对应币对
    @PostMapping("/remove/ticker")
    public int removeTicker(
        @RequestParam Long orgId,
        @RequestParam Long exchangeId,
        @RequestParam String symbol) {
        Set<Symbol> symbolSet = new HashSet<>();
        symbolSet.add(Symbol.newBuilder()
            .setExchangeId(exchangeId)
            .setSymbolId(symbol)
            .build());
        for (int symbolType : BrokerInfoTask.SYMBOL_TYPE_ARRAY) {
            for (RealtimeIntervalEnum realtimeIntervalEnum : RealtimeIntervalEnum.values()) {
                try {
                    TopNListener topNListener = context.getBean(BeanUtils.getBrokerTopNBeanName(orgId, symbolType, realtimeIntervalEnum.getInterval()),
                        TopNListener.class);
                    topNListener.removeSymbols(symbolSet);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }

                try {
                    BrokerListener brokerListener = context.getBean(BeanUtils.getBrokerTopNBeanName(orgId, symbolType, realtimeIntervalEnum.getInterval()),
                        BrokerListener.class);
                    brokerListener.removeSymbols(symbolSet);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }

                try {
                    SlowBrokerListener slowBrokerListener = context.getBean(BeanUtils.getSlowBrokerBeanName(orgId, symbolType, realtimeIntervalEnum.getInterval()),
                        SlowBrokerListener.class);
                    slowBrokerListener.removeSymbols(symbolSet);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
        return 0;
    }

    @PostMapping("/online")
    public int online(@RequestParam boolean open) {
        if (commonConfig.isOnline() != open) {
            commonConfig.setOnline(open);
        }
        return 0;
    }

    @PostMapping("/close/subscription")
    public int closeSubscription(@RequestParam Long exchangeId, @RequestParam String symbol) {
        return 0;
    }
}
