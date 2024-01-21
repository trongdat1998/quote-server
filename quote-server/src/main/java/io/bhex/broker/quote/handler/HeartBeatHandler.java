package io.bhex.broker.quote.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HeartBeatHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof PingWebSocketFrame) {
            PingWebSocketFrame frame = (PingWebSocketFrame) msg;
            try {
                ctx.channel().writeAndFlush(new PongWebSocketFrame(frame.content().retain()));
            } catch (Exception e) {
                log.error("", e);
            } finally {
                frame.release();
            }
            return;
        }
        if (msg instanceof PongWebSocketFrame) {
            PongWebSocketFrame frame = (PongWebSocketFrame) msg;
            try {
                ctx.channel().writeAndFlush(new PingWebSocketFrame(frame.content().retain()));
            } catch (Exception e) {
                log.error("", e);
            } finally {
                frame.release();
            }
            return;
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
    }
}
