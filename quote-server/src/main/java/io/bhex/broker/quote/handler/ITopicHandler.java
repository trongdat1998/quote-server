package io.bhex.broker.quote.handler;

import io.bhex.broker.quote.data.ClientMessageV2;
import io.bhex.broker.quote.enums.TopicEnum;
import io.netty.channel.ChannelHandlerContext;

public interface ITopicHandler {
    void handle(ChannelHandlerContext ctx, ClientMessageV2 msg);

    boolean isSameTopic(ClientMessageV2 msg);
}
