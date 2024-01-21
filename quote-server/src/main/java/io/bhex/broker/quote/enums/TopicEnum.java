package io.bhex.broker.quote.enums;

import io.bhex.broker.quote.listener.BrokerListener;
import io.bhex.broker.quote.listener.DepthListener;
import io.bhex.broker.quote.listener.DiffDepthListener;
import io.bhex.broker.quote.listener.DiffMergedDepthListener;
import io.bhex.broker.quote.listener.IndexKlineListener;
import io.bhex.broker.quote.listener.IndexListener;
import io.bhex.broker.quote.listener.KlineListener;
import io.bhex.broker.quote.listener.MarkPriceListener;
import io.bhex.broker.quote.listener.MergedDepthListener;
import io.bhex.broker.quote.listener.RealTimeListener;
import io.bhex.broker.quote.listener.SlowBrokerListener;
import io.bhex.broker.quote.listener.TopNListener;
import io.bhex.broker.quote.listener.TradeListener;
import io.bhex.broker.quote.listener.v2.BookTickerListener;

public enum TopicEnum {
    trade(3, TradeListener.class, true),
    realtimes(4, RealTimeListener.class, true),
    kline(5, KlineListener.class, true),
    diffDepth(6, DiffDepthListener.class, true),
    mergedDepth(7, MergedDepthListener.class, true),
    depth(8, DepthListener.class, true),
    diffMergedDepth(9, DiffMergedDepthListener.class, true),
    index(10, IndexListener.class, false),
    topN(11, TopNListener.class, false),
    broker(12, BrokerListener.class, false),
    bookTicker(13, BookTickerListener.class, false),
    slowBroker(14, SlowBrokerListener.class, false),
    markPrice(15, MarkPriceListener.class, false),
    indexKline(17, IndexKlineListener.class, true),
    ;

    public int code;
    public Class listenerClazz;
    /**
     * 重连时不需要再添加schedule任务
     * 有的些类型需要重新加载一次cache，但不是scheduler
     */
    public boolean isNeedReloadCache;

    TopicEnum(int code, Class listenerClazz, boolean isNeedReloadCache) {
        this.code = code;
        this.listenerClazz = listenerClazz;
        this.isNeedReloadCache = isNeedReloadCache;
    }

    public static TopicEnum valueOF(String name) {
        for (TopicEnum topicEnum : values()) {
            if (topicEnum.name().equals(name)) {
                return topicEnum;
            }
        }
        return null;
    }
}
