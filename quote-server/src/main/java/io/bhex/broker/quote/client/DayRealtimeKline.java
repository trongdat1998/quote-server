package io.bhex.broker.quote.client;

import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.KLine;
import io.bhex.base.quote.Realtime;
import io.bhex.broker.quote.listener.BrokerListener;
import io.bhex.broker.quote.listener.RealTimeListener;
import io.bhex.broker.quote.listener.SlowBrokerListener;
import io.bhex.broker.quote.listener.TopNListener;
import io.bhex.broker.quote.repository.SymbolRepository;
import io.bhex.broker.quote.util.BeanUtils;
import io.bhex.broker.quote.util.ConstantUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.Set;

import static io.bhex.broker.quote.listener.TopNListener.ALL;

/**
 * @author wangsc
 * @description 日涨跌幅工具(为了方便任意地方使用 ， 不使用spring的bean创建)
 * 其实理论上是可以基于spring的bean
 * 如果频繁修改会造成涨跌幅数据异常(暂时忽略)
 * @date 2020-07-15 10:26
 */
@Slf4j
@Component
public class DayRealtimeKline {

    @Resource
    private ApplicationContext context;

    @Resource
    private SymbolRepository symbolRepository;

    private void handleRealTimeListener(Realtime realtime, String realtimeInterval) {
        //处理v1RealTimeListener
        RealTimeListener realTimeListener = context.getBean(BeanUtils.getRealTimeBeanName(realtime.getExchangeId(), realtime.getS(), realtimeInterval), RealTimeListener.class);
        realTimeListener.onEngineMessage(realtime);
        //处理v2RealTimeListener
        io.bhex.broker.quote.listener.v2.RealTimeListener v2RealTimeListener = context.getBean(BeanUtils
                        .getTickerV2BeanName(realtime.getExchangeId(), realtime.getS(), realtimeInterval),
                io.bhex.broker.quote.listener.v2.RealTimeListener.class);
        v2RealTimeListener.onMessage(realtime);
    }

    /**
     * 传入日k相关数据(后期看情况扩展),异步处理
     */
    public void handleDayKline(KLine kline, String realtimeInterval) {
        Set<Long> getSharedExchangeIds = symbolRepository.getReversedSharedExchangeIds(kline.getExchangeId(),
                kline.getSymbol());
        Set<Long> orgIds = symbolRepository.getOrgIdsByExchangeIdAndSymbol(kline.getExchangeId(),
                kline.getSymbol());
        for (Long getSharedExchangeId : getSharedExchangeIds) {
            orgIds.addAll(symbolRepository.getOrgIdsByExchangeIdAndSymbol(getSharedExchangeId, kline.getSymbol()));
        }
        //过滤所有支持当前
        if (!orgIds.isEmpty()) {
            //转换realtime
            Realtime realtime = getRealtimeByKline(kline);
            //更新RealTimeListener
            handleRealTimeListener(realtime, realtimeInterval);
            for (Long orgId : orgIds) {
                int curType = symbolRepository.getSymbolType(kline.getSymbol());
                if (curType == 0) {
                    log.error("Unknown symbol type [{}:{}]", kline.getExchangeId(), kline.getSymbol());
                    continue;
                }
                try {
                    TopNListener topNListener = (TopNListener) context.getBean(BeanUtils.getBrokerTopNBeanName(orgId,
                            curType, realtimeInterval));
                    topNListener.onEngineMessage(realtime);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }

                try {
                    TopNListener topNListener = (TopNListener) context.getBean(BeanUtils.getBrokerTopNBeanName(orgId,
                            ALL, realtimeInterval));
                    topNListener.onEngineMessage(realtime);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }

                try {
                    BrokerListener brokerListener = (BrokerListener) context.getBean(BeanUtils.getBrokerBeanName(orgId,
                            curType, realtimeInterval));
                    brokerListener.onEngineMessage(realtime);
                    SlowBrokerListener slowBrokerListener = context.getBean(BeanUtils.getSlowBrokerBeanName(orgId,
                            curType, realtimeInterval), SlowBrokerListener.class);
                    slowBrokerListener.onEngineMessage(realtime);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }

                try {
                    BrokerListener brokerListener = (BrokerListener) context.getBean(BeanUtils.getBrokerBeanName(orgId,
                            ALL, realtimeInterval));
                    brokerListener.onEngineMessage(realtime);
                    SlowBrokerListener slowBrokerListener = context.getBean(BeanUtils.
                            getSlowBrokerBeanName(orgId, ALL, realtimeInterval), SlowBrokerListener.class);
                    slowBrokerListener.onEngineMessage(realtime);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * 不限制间隔，后续可能有任意时间开始算涨跌幅的需求
     *
     * @param kLine kline
     */
    private Realtime getRealtimeByKline(KLine kLine) {
        if (kLine == null) {
            return null;
        }
        if (ConstantUtil.UNIQUE_ZERO.compareTo(DecimalUtil.toBigDecimal(kLine.getOpen())) == 0) {
            log.error("Error kline data! id:{},exchangeId:{},symbolId:{},open:{},close:{},interval:{}", kLine.getId(), kLine.getExchangeId(), kLine.getSymbol(), kLine.getOpen(), kLine.getClose(), kLine.getInterval());
        }
        return Realtime.newBuilder()
                .setT(String.valueOf(Instant.now().toEpochMilli()))
                .setS(kLine.getSymbol())
                .setC(kLine.getClose().getStr())
                .setH(DecimalUtil.toBigDecimal(kLine.getHigh()).toPlainString())
                .setL(DecimalUtil.toBigDecimal(kLine.getLow()).toPlainString())
                .setO(DecimalUtil.toBigDecimal(kLine.getOpen()).toPlainString())
                .setV(DecimalUtil.toBigDecimal(kLine.getVolume()).toPlainString())
                .setTrs(String.valueOf(kLine.getTrades()))
                .setQv(DecimalUtil.toBigDecimal(kLine.getQuoteVolume()).toPlainString())
                .setTkBv(DecimalUtil.toBigDecimal(kLine.getTakerBaseVolume()).toPlainString())
                .setTkQv(DecimalUtil.toBigDecimal(kLine.getTakerQuoteVolume()).toPlainString())
                .setExchangeId(kLine.getExchangeId())
                .build();
    }

}
