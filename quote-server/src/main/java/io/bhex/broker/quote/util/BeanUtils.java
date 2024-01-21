package io.bhex.broker.quote.util;

import io.bhex.broker.quote.enums.TopicEnum;
import org.apache.commons.lang3.StringUtils;

import static ch.qos.logback.core.CoreConstants.DOT;
import static io.bhex.broker.quote.common.SymbolConstants.UNDERLINE;

public class BeanUtils {
    private static final String BEAN_V2_PREFIX = "V2_";

    public static String getMarkPriceBeanName(Long exchangeId, String symbol) {
        return TopicEnum.markPrice.name() + exchangeId + DOT + symbol;
    }

    public static String getIndexKlineBeanName(String symbol, String interval) {
        return TopicEnum.indexKline + UNDERLINE + interval + DOT + symbol;
    }

    public static String getDepthV2BeanName(Long exchangeId, String symbol) {
        return BEAN_V2_PREFIX + getDepthBeanName(exchangeId, symbol);
    }

    public static String getTradeV2BeanName(Long exchangeId, String symbol) {
        return BEAN_V2_PREFIX + getTradesBeanName(exchangeId, symbol);
    }

    public static String getTickerV2BeanName(Long exchangeId, String symbol, String realtimeInterval) {
        return BEAN_V2_PREFIX + getRealTimeBeanName(exchangeId, symbol, realtimeInterval);
    }

    public static String getKlineV2BeanName(Long exchangeId, String symbol, String interval) {
        return BEAN_V2_PREFIX + getKlineBeanName(exchangeId, symbol, interval);
    }

    public static String getBookTickerV2BeanName(Long exchangeId, String symbol) {
        return BEAN_V2_PREFIX + TopicEnum.bookTicker.name() + exchangeId + DOT + symbol;
    }

    public static String getBrokerBeanName(Long orgId, int symbolType, String realtimeInterval) {
        return TopicEnum.broker.name() + UNDERLINE + orgId + UNDERLINE + symbolType + UNDERLINE + realtimeInterval;
    }

    public static String getSlowBrokerBeanName(Long orgId, int symbolType, String realtimeInterval) {
        return TopicEnum.slowBroker.name() + UNDERLINE + orgId + UNDERLINE + symbolType + UNDERLINE + realtimeInterval;
    }

    public static String getBrokerTopNBeanName(Long orgId, int symbolType, String realtimeInterval) {
        return TopicEnum.topN.name() + UNDERLINE + orgId + UNDERLINE + symbolType + UNDERLINE + realtimeInterval;
    }

    public static String getRealTimeBeanName(Long exchangeId, String symbol, String realtimeInterval) {
        return TopicEnum.realtimes.name() + exchangeId + DOT + symbol + DOT + realtimeInterval;
    }

    public static String getTradesBeanName(Long exchangeId, String symbol) {
        return TopicEnum.trade.name() + exchangeId + DOT + symbol;
    }

    public static String getDepthBeanName(Long exchangeId, String symbol) {
        return TopicEnum.depth.name() + exchangeId + DOT + symbol;
    }

    public static String getDepthBeanName(Long exchangeId, String symbol, Integer dumpScale) {
        return TopicEnum.mergedDepth.name() + exchangeId + DOT + symbol + dumpScale;
    }

    public static String getDiffDepthBeanName(Long exchangeId, String symbol) {
        return TopicEnum.diffDepth.name() + exchangeId + DOT + symbol;
    }

    public static String getDiffDepthBeanName(Long exchangeId, String symbol, Integer dumpScale) {
        return TopicEnum.diffMergedDepth.name() + exchangeId + DOT + symbol + dumpScale;
    }

    public static String getKlineBeanName(Long exchangeId, String symbol, String interval) {
        return TopicEnum.kline.name() + UNDERLINE + interval + exchangeId + DOT + symbol;
    }

    public static String getBeanNameByTopicWithExchangeIdAndSymbol(Long exchangeId, String symbol, TopicEnum topicEnum) {
        switch (topicEnum) {
            case diffDepth:
                return getDiffDepthBeanName(exchangeId, symbol);
            case depth:
                return getDepthBeanName(exchangeId, symbol);
            case trade:
                return getTradesBeanName(exchangeId, symbol);
            default:
                return StringUtils.EMPTY;
        }
    }

}
