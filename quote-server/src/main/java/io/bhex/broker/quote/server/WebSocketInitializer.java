package io.bhex.broker.quote.server;

import io.bhex.broker.quote.handler.ClientMessageV2Handler;
import io.bhex.broker.quote.handler.ClientMessageHandler;
import io.bhex.broker.quote.handler.HeartBeatHandler;
import io.bhex.broker.quote.handler.MessageHandler;
import io.bhex.broker.quote.handler.WebSocketOutHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.timeout.IdleStateHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class WebSocketInitializer extends ChannelInitializer<SocketChannel> {
    private static final int MAX_BODY_SIZE = 65535;

    private final ApplicationContext context;

    @Autowired
    public WebSocketInitializer(ApplicationContext context) {
        this.context = context;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ch.pipeline()
            .addLast(new HttpServerCodec())
            .addLast(new HttpObjectAggregator(MAX_BODY_SIZE))
            .addLast(new IdleStateHandler(300, 0, 0))
            // 心跳
            .addLast(new HeartBeatHandler())
            // 转成ClientMessage
            .addLast(new MessageHandler(context))
            .addLast(new ClientMessageHandler(context))
            .addLast(new ClientMessageV2Handler(context))
            .addLast(new WebSocketOutHandler());
    }
}
