package io.bhex.broker.quote.client;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;

import static io.bhex.broker.quote.client.QuoteMessageCodec.delimiter;

public class QuoteEngineInitializer extends ChannelInitializer<SocketChannel> {
    private QuoteResponseHandler quoteResponseHandler;

    public QuoteEngineInitializer(QuoteResponseHandler quoteResponseHandler) {
        this.quoteResponseHandler = quoteResponseHandler;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline()
            .addLast(new DelimiterBasedFrameDecoder(1024 * 1024, Unpooled.wrappedBuffer(delimiter.getBytes())))
            .addLast(new QuoteMessageCodec())
            .addLast(quoteResponseHandler);
    }
}
