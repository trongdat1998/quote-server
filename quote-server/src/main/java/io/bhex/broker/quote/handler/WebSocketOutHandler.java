package io.bhex.broker.quote.handler;

import io.bhex.broker.quote.data.ClientMessage;
import io.bhex.broker.quote.data.ClientMessageV2;
import io.bhex.broker.quote.data.Pong;
import io.bhex.broker.quote.enums.ErrorCodeEnum;
import io.bhex.broker.quote.util.JsonUtils;
import io.bhex.broker.quote.util.ZipUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.BooleanUtils;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

import static io.bhex.broker.quote.enums.StringsConstants.BINARY;

/**
 */
@Slf4j
public class WebSocketOutHandler extends ChannelOutboundHandlerAdapter {

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
        throws Exception {
        if (!ctx.channel().isWritable()) {
            ctx.channel().close();
            return;
        }
        if (Objects.isNull(msg)) {
            return;
        }
        if (!ctx.channel().isActive()) {
            ctx.channel().close();
            return;
        }
        if (msg instanceof Pong) {
            writeString(ctx, JsonUtils.toJSONString(msg));
            return;
        }
        if (msg instanceof ClientMessage) {
            writeMessage(ctx, (ClientMessage) msg);
            return;
        }
        if (msg instanceof ClientMessageV2) {
            writeMessage(ctx, (ClientMessageV2) msg);
        }
        if (msg instanceof ErrorCodeEnum) {
            writeString(ctx, msg.toString());
            return;
        }
        if (msg instanceof String) {
            writeBinary(ctx, (String)msg);
            return;
        }
        ctx.writeAndFlush(msg);
    }

    private void writeBinary(ChannelHandlerContext ctx, String msg) {
        byte[] binaryData = ZipUtil.gzip(msg);
        ctx.channel().writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(binaryData)));
    }

    private void writeString(ChannelHandlerContext ctx, String msg) throws UnsupportedEncodingException {
        TextWebSocketFrame textWF = new TextWebSocketFrame();
        textWF.content().writeBytes(msg.getBytes(StandardCharsets.UTF_8));
        ctx.channel().writeAndFlush(textWF);
    }

    private void writeMessage(ChannelHandlerContext ctx, ClientMessage message) throws UnsupportedEncodingException {
        String msg = JsonUtils.toJSONString(message);
        if (Objects.nonNull(message) && Objects.nonNull(message.getParams())) {
            Map<String, String> params = message.getParams();
            boolean isBinary = BooleanUtils.toBoolean(params.get(BINARY));
            if (isBinary) {
                writeBinary(ctx, msg);
                return;
            }
        }
        writeString(ctx, msg);
    }

    private void writeMessage(ChannelHandlerContext ctx, ClientMessageV2 message) throws UnsupportedEncodingException {
        String msg = JsonUtils.toJSONString(message);
        if (Objects.nonNull(message) && Objects.nonNull(message.getParams())) {
            Map<String, String> params = message.getParams();
            boolean isBinary = BooleanUtils.toBoolean(params.get(BINARY));
            if (isBinary) {
                writeBinary(ctx, msg);
                return;
            }
        }
        writeString(ctx, msg);
    }
}
