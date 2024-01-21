package io.bhex.broker.quote.handler;

import io.bhex.broker.quote.data.ClientMessageV2;
import io.bhex.broker.quote.enums.ErrorCodeEnum;
import io.bhex.broker.quote.enums.EventEnum;
import io.bhex.broker.quote.enums.RealtimeIntervalEnum;
import io.bhex.broker.quote.exception.BizException;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.bhex.broker.quote.enums.StringsConstants.REALTIME_INTERVAL;

@Slf4j
public class ClientMessageV2Handler extends SimpleChannelInboundHandler<ClientMessageV2> {
    private ApplicationContext context;
    private Map<String, ITopicHandler> topicHandlerMap;

    public ClientMessageV2Handler(ApplicationContext applicationContext) {
        this.context = applicationContext;
        this.topicHandlerMap = context.getBeansOfType(ITopicHandler.class);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ClientMessageV2 msg) throws Exception {
        if (StringUtils.isEmpty(msg.getTopic())) {
            throw new BizException(ErrorCodeEnum.REQUIRED_TOPIC);
        }

        if (StringUtils.isEmpty(msg.getEvent())) {
            throw new BizException(ErrorCodeEnum.REQUIRED_EVENT);
        }

        EventEnum event = EventEnum.valueOF(msg.getEvent());
        if (Objects.isNull(event)) {
            throw new BizException(ErrorCodeEnum.INVALID_EVENT);
        }

        //更换realTime为标准值
        Map<String, String> params = msg.getParams();
        if (params == null) {
            params = new HashMap<>(2);
            msg.setParams(params);
        }
        RealtimeIntervalEnum realtimeIntervalEnum = RealtimeIntervalEnum.intervalOf(params.get(REALTIME_INTERVAL));
        params.put(REALTIME_INTERVAL, realtimeIntervalEnum.getInterval());
        for (Map.Entry<String, ITopicHandler> stringITopicHandlerEntry : topicHandlerMap.entrySet()) {
            ITopicHandler handler = stringITopicHandlerEntry.getValue();
            if (handler.isSameTopic(msg)) {
                handler.handle(ctx, msg);
                return;
            }
        }
        throw new BizException(ErrorCodeEnum.INVALID_TOPIC);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof BizException) {
            ctx.channel().writeAndFlush(((BizException) cause).getErrorCodeEnum());
            return;
        }

        if (cause instanceof NoSuchBeanDefinitionException) {
            ctx.channel().writeAndFlush(ErrorCodeEnum.SYMBOLS_ERROR);
            return;
        }
        log.error(cause.getMessage(), cause);
        super.exceptionCaught(ctx, cause);
    }

    public void cancelAll(ChannelHandlerContext ctx, ClientMessageV2 msg) {
        for (Map.Entry<String, ITopicHandler> stringITopicHandlerEntry : topicHandlerMap.entrySet()) {
            ITopicHandler handler = stringITopicHandlerEntry.getValue();
            handler.handle(ctx, msg);
        }
    }
}
