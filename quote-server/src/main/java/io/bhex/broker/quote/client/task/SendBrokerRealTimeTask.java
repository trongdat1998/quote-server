package io.bhex.broker.quote.client.task;

import io.bhex.base.quote.Realtime;
import io.bhex.broker.quote.enums.RealtimeIntervalEnum;
import io.bhex.broker.quote.listener.BrokerListener;
import io.bhex.broker.quote.listener.SlowBrokerListener;
import io.bhex.broker.quote.listener.TopNListener;
import io.bhex.broker.quote.repository.SymbolRepository;
import io.bhex.broker.quote.util.BeanUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;

import java.util.Set;

import static io.bhex.broker.quote.listener.TopNListener.ALL;

@Slf4j
public class SendBrokerRealTimeTask implements Runnable {
    private SymbolRepository symbolRepository;
    private ApplicationContext context;
    private Realtime realtime;

    public SendBrokerRealTimeTask(SymbolRepository symbolRepository, ApplicationContext context, Realtime realtime) {
        this.symbolRepository = symbolRepository;
        this.context = context;
        this.realtime = realtime;
    }

    @Override
    public void run() {
        try {
            Set<Long> getSharedExchangeIds = symbolRepository.getReversedSharedExchangeIds(realtime.getExchangeId(),
                realtime.getS());
            Set<Long> orgIds = symbolRepository.getOrgIdsByExchangeIdAndSymbol(realtime.getExchangeId(), realtime.getS());
            for (Long getSharedExchangeId : getSharedExchangeIds) {
                orgIds.addAll(symbolRepository.getOrgIdsByExchangeIdAndSymbol(getSharedExchangeId, realtime.getS()));
            }
            for (Long orgId : orgIds) {
                int curType = symbolRepository.getSymbolType(realtime.getS());
                if (curType == 0) {
                    log.error("Unknown symbol type [{}:{}]", realtime.getExchangeId(), realtime.getS());
                    continue;
                }
                try {
                    TopNListener topNListener = (TopNListener) context.getBean(BeanUtils.getBrokerTopNBeanName(orgId,
                        curType, RealtimeIntervalEnum.H24.getInterval()));
                    topNListener.onEngineMessage(realtime);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }

                try {
                    TopNListener topNListener = (TopNListener) context.getBean(BeanUtils.getBrokerTopNBeanName(orgId,
                         ALL, RealtimeIntervalEnum.H24.getInterval()));
                    topNListener.onEngineMessage(realtime);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }

                try {
                    BrokerListener brokerListener = (BrokerListener) context.getBean(BeanUtils.getBrokerBeanName(orgId,
                        curType, RealtimeIntervalEnum.H24.getInterval()));
                    brokerListener.onEngineMessage(realtime);
                    SlowBrokerListener slowBrokerListener = context.getBean(BeanUtils.getSlowBrokerBeanName(orgId,
                        curType, RealtimeIntervalEnum.H24.getInterval()), SlowBrokerListener.class);
                    slowBrokerListener.onEngineMessage(realtime);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }

                try {
                    BrokerListener brokerListener = (BrokerListener) context.getBean(BeanUtils.getBrokerBeanName(orgId,
                        ALL, RealtimeIntervalEnum.H24.getInterval()));
                    brokerListener.onEngineMessage(realtime);
                    SlowBrokerListener slowBrokerListener = context.getBean(BeanUtils.
                        getSlowBrokerBeanName(orgId, ALL, RealtimeIntervalEnum.H24.getInterval()), SlowBrokerListener.class);
                    slowBrokerListener.onEngineMessage(realtime);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

}
