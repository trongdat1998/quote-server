package io.bhex.broker.quote.listener;

import io.bhex.broker.quote.data.ClientMessage;
import io.bhex.broker.quote.enums.TopicEnum;
import io.netty.channel.Channel;

/**
 *
 */
public interface DataListener<T> {
    /**
     * 数据更新事件
     */
    void onEngineMessage(T data);

    /**
     * 增加订阅者
     */
    void addSubscriber(Channel channel, ClientMessage clientMessage);

    /**
     * 删除订阅者
     */
    void removeSubscriber(Channel channel);

    void watching();

    TopicEnum getTopic();

    Long getExchangeId();

    String getSymbol();

    String getSymbols();

    void clearSnapshot();

    void stop();
}
