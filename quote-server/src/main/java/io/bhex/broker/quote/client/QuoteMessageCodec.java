package io.bhex.broker.quote.client;

import io.bhex.base.quote.QuoteRequest;
import io.bhex.base.quote.QuoteResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class QuoteMessageCodec extends MessageToMessageCodec<ByteBuf, QuoteRequest> {
    public static final String delimiter = "$B$";

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, QuoteRequest quoteRequest, List<Object> list) throws Exception {
        byte[] bytes = quoteRequest.toByteArray();
        list.add(Unpooled.wrappedBuffer(bytes));
        list.add(Unpooled.wrappedBuffer(delimiter.getBytes()));
    }

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf request, List<Object> list) throws Exception {
        if (request.isReadable()) {
            byte[] bytes;
            int length = request.readableBytes();
            if (request.hasArray()) {
                bytes = request.array();
            } else {
                bytes = new byte[length];
                request.getBytes(request.readerIndex(), bytes);
            }

            try {
                list.add(QuoteResponse.parseFrom(bytes));
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

    }
}
