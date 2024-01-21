package io.bhex.broker.quote.listener.v2;

import io.bhex.broker.quote.data.ClientMessageV2;
import io.bhex.broker.quote.enums.TopicEnum;
import io.netty.channel.Channel;

public interface ITopicListener<T> {

    TopicEnum getTopic();

    void subscribe(Channel channel, ClientMessageV2 msg);

    void unsubscribe(Channel channel, ClientMessageV2 msg);

    void onMessage(T t);

    void watch();
}
