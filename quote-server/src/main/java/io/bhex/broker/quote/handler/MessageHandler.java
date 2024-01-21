package io.bhex.broker.quote.handler;


import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.collect.Sets;
import io.bhex.broker.quote.data.ClientMessage;
import io.bhex.broker.quote.data.ClientMessageV2;
import io.bhex.broker.quote.data.Pong;
import io.bhex.broker.quote.enums.ErrorCodeEnum;
import io.bhex.broker.quote.metrics.PushMetrics;
import io.bhex.broker.quote.repository.SymbolRepository;
import io.bhex.broker.quote.util.JsonUtils;
import io.bhex.broker.quote.util.Utils;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketHandshakeException;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshakerFactory;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.AttributeKey;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.springframework.context.ApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.handler.AbstractHandlerMapping;

import java.io.StringWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.lambdaworks.redis.protocol.CommandType.PING;
import static io.bhex.broker.quote.QuoteWsApplication.ORG_ID_VALUE;
import static io.bhex.broker.quote.enums.StringsConstants.BHEX_QUOTE_KEY;
import static io.bhex.broker.quote.enums.StringsConstants.ORG_ID;
import static io.bhex.broker.quote.enums.StringsConstants.QUOTE_WS_VERSION;
import static io.netty.handler.codec.http.HttpHeaderNames.HOST;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.http.HttpVersion.HTTP;

@Slf4j
public class MessageHandler extends ChannelDuplexHandler {
    private WebSocketServerHandshaker handshaker;

    private ApplicationContext context;
    private AbstractHandlerMapping abstractHandlerMapping;
    private DispatcherServlet servlet;
    /*
    Thr uri supported
     */
    private static final String OPEN_WS_URI = "/openapi/quote/ws/v1";
    private static final String OPEN_WS_URI_V1 = "/openapi/ws/quote/v1";
    private static final String OPEN_WS_URI_V2 = "/openapi/quote/ws/v2";
    private static final String WS_URI = "/ws/quote/v1";

    /*
    internal endpoint
     */
    private static final String HEALTH_CHECK = "/internal/health";
    private static final String METRICS = "/internal/metrics";

    private SymbolRepository symbolRepository;

    public MessageHandler(ApplicationContext context) {
        this.context = context;

        this.servlet = context.getBean(DispatcherServlet.class);
        this.symbolRepository = context.getBean(SymbolRepository.class);
        //HandlerAdapter ha = getHandlerAdapter(mappedHandler.getHandler());
        //abstractHandlerMapping.getHandler().getHandler()
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        PushMetrics.addChannel();
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        PushMetrics.removeChannel();
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof FullHttpRequest) {
            FullHttpRequest fullHttpRequest = (FullHttpRequest) msg;
            try {
                handleHttpRequest(ctx, fullHttpRequest);
            } catch (Exception e) {
                log.error("Handle http ex:", e);
            } finally {
                fullHttpRequest.release();
            }
            return;
        }
        // Check for closing frame
        if (msg instanceof CloseWebSocketFrame) {
            CloseWebSocketFrame frame = (CloseWebSocketFrame) msg;
            try {
                handshaker.close(ctx.channel(), frame.retain());
            } catch (Exception e) {
                log.error("", e);
            } finally {
                frame.release();
            }
            return;
        }

        if (!(msg instanceof TextWebSocketFrame)) {
            log.warn("Not support type: [{}]", msg.getClass().getName());
            return;
        }

        //web socket操作
        String text = null;
        try {
            text = ((TextWebSocketFrame) msg).text();
            if (StringUtils.isEmpty(text)) {
                ctx.channel().writeAndFlush(ErrorCodeEnum.INVALID_REQUEST);
                return;
            }
            if (StringUtils.containsIgnoreCase(text, PING.name())) {
                ctx.channel().writeAndFlush(new Pong(System.currentTimeMillis()));
                return;
            }

            if (Utils.getWsVersion(ctx) == 2) {
                ClientMessageV2 clientMessageV2 = JsonUtils.parseJson(text, new TypeReference<ClientMessageV2>() {
                });
                if (Objects.isNull(clientMessageV2)) {
                    ctx.channel().writeAndFlush(ErrorCodeEnum.JSON_FORMAT_ERROR);
                    return;
                }
                ctx.fireChannelRead(clientMessageV2);
                return;
            }

            ClientMessage clientMessage = JsonUtils.parseJson(text, new TypeReference<ClientMessage>() {
            });
            if (Objects.isNull(clientMessage)) {
                ctx.channel().writeAndFlush(ErrorCodeEnum.JSON_FORMAT_ERROR);
                return;
            }

            ctx.fireChannelRead(clientMessage);

        } catch (JsonParseException | JsonMappingException jsonException) {
            ctx.channel().writeAndFlush(ErrorCodeEnum.JSON_FORMAT_ERROR);
            log.debug("jsonException exception text [{}] ", text);
        } catch (Exception e) {
            log.error("subPublish ex: ", e);
        } finally {
            ((WebSocketFrame) msg).release();
        }
    }

    private void handleHttpRequest(ChannelHandlerContext ctx, FullHttpRequest req) {
        // Handle a bad request.
        if (!req.decoderResult().isSuccess()) {
            sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST));
            return;
        }
        // Allow only GET methods.
        if (req.method() != GET) {
            sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, METHOD_NOT_ALLOWED));
            return;
        }

        boolean isV2 = false;
        if (StringUtils.startsWith(req.uri(), OPEN_WS_URI_V2)) {
            isV2 = true;
        }

        // health check
        if (req.protocolVersion().text().startsWith(HTTP)
            && !req.uri().startsWith(WS_URI) && !req.uri().startsWith(OPEN_WS_URI)
            && !req.uri().startsWith(OPEN_WS_URI_V1) && !isV2) {

            req.content();
            if (req.uri().startsWith(HEALTH_CHECK)) {
                sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, OK,
                    Unpooled.wrappedBuffer(ErrorCodeEnum.OK.name().getBytes())));
            } else if (req.uri().startsWith(METRICS)) {
                handleMetrics(ctx, req);
            } else {
                sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, NOT_FOUND));
            }
            return;
        }
        // Handshake
        WebSocketServerHandshakerFactory wsFactory = new WebSocketServerHandshakerFactory(getWebSocketLocation(req),
            null, false);
        handshaker = wsFactory.newHandshaker(req);
        if (handshaker == null) {
            WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel());
        } else {
            try {
                handshaker.handshake(ctx.channel(), req);
                List<String> hostList = req.headers().getAll(HttpHeaders.HOST);
                String host = CollectionUtils.isEmpty(hostList) ? null : hostList.get(hostList.size() - 1);
                if (isV2) {
                    ctx.channel().attr(AttributeKey.valueOf(QUOTE_WS_VERSION)).set(2);
                }
                ctx.channel().attr(AttributeKey.valueOf(HttpHeaders.HOST)).set(host);
                ctx.channel().attr(AttributeKey.valueOf(BHEX_QUOTE_KEY)).set(req.headers().get(BHEX_QUOTE_KEY));
                ctx.channel().attr(AttributeKey.valueOf(ORG_ID)).set(ORG_ID_VALUE);
            } catch (WebSocketHandshakeException e) {
                sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST,
                    Unpooled.wrappedBuffer("Missing websocket-key".getBytes())));
            }
        }
    }

    private void sendHttpResponse(ChannelHandlerContext ctx, FullHttpRequest req, FullHttpResponse res) {
        HttpUtil.setContentLength(res, res.content().readableBytes());
        // Send the response and close the connection if necessary.
        ChannelFuture f = ctx.channel().writeAndFlush(res);
        if (!HttpUtil.isKeepAlive(req) || res.status().code() != 200) {
            f.addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            if (e.state() == IdleState.READER_IDLE) {
                ctx.close();
            } else if (e.state() == IdleState.WRITER_IDLE) {
                ctx.close();
            }
        }
    }


    private static String getWebSocketLocation(FullHttpRequest req) {
        return "ws://" + req.headers().get(HOST);
    }

    private void handleMetrics(ChannelHandlerContext ctx, FullHttpRequest req) {
        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(req.uri());
        List<String> names = queryStringDecoder.parameters().get("names");
        Writer writer = new StringWriter();
        FullHttpResponse res = null;
        try {
            Set<String> includedNameSet = names == null ? Collections.emptySet() : Sets.newHashSet(names);
            Enumeration<Collector.MetricFamilySamples> prometheusSamples = CollectorRegistry.defaultRegistry
                .filteredMetricFamilySamples(includedNameSet);
            TextFormat.write004(writer, prometheusSamples);
            String content = writer.toString();
            res = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(content.getBytes()));
        } catch (Exception e) {
            log.error("handle ex:", e);
            res = new DefaultFullHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR);
        }
        HttpUtil.setContentLength(res, res.content().readableBytes());
        // Send the response and close the connection if necessary.
        ChannelFuture f = ctx.channel().writeAndFlush(res);
        if (!HttpUtil.isKeepAlive(req) || res.status().code() != 200) {
            f.addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
    }
}
