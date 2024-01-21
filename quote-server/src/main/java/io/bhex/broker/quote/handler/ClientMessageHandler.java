package io.bhex.broker.quote.handler;

import io.bhex.base.quote.IndexConfig;
import io.bhex.broker.quote.data.ClientMessage;
import io.bhex.broker.quote.data.Symbols;
import io.bhex.broker.quote.enums.ErrorCodeEnum;
import io.bhex.broker.quote.enums.EventEnum;
import io.bhex.broker.quote.enums.RealtimeIntervalEnum;
import io.bhex.broker.quote.enums.StringsConstants;
import io.bhex.broker.quote.enums.TopicEnum;
import io.bhex.broker.quote.exception.BizException;
import io.bhex.broker.quote.listener.BrokerListener;
import io.bhex.broker.quote.listener.DataListener;
import io.bhex.broker.quote.listener.SlowBrokerListener;
import io.bhex.broker.quote.listener.TopNListener;
import io.bhex.broker.quote.repository.BhServerRepository;
import io.bhex.broker.quote.repository.SymbolRepository;
import io.bhex.broker.quote.util.BeanUtils;
import io.bhex.exchange.enums.KlineIntervalEnum;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import static ch.qos.logback.core.CoreConstants.DOT;
import static io.bhex.broker.quote.enums.StringsConstants.BINARY;
import static io.bhex.broker.quote.enums.StringsConstants.DUMP_SCALE;
import static io.bhex.broker.quote.enums.StringsConstants.KLINE_TYPE;
import static io.bhex.broker.quote.enums.StringsConstants.LIMIT;
import static io.bhex.broker.quote.enums.StringsConstants.ORG;
import static io.bhex.broker.quote.enums.StringsConstants.ORG_ID;
import static io.bhex.broker.quote.enums.StringsConstants.REALTIME_INTERVAL;
import static io.bhex.broker.quote.enums.StringsConstants.TYPE;
import static io.bhex.broker.quote.enums.TopicEnum.broker;
import static io.bhex.broker.quote.enums.TopicEnum.diffMergedDepth;
import static io.bhex.broker.quote.enums.TopicEnum.index;
import static io.bhex.broker.quote.enums.TopicEnum.indexKline;
import static io.bhex.broker.quote.enums.TopicEnum.kline;
import static io.bhex.broker.quote.enums.TopicEnum.mergedDepth;
import static io.bhex.broker.quote.enums.TopicEnum.realtimes;
import static io.bhex.broker.quote.enums.TopicEnum.slowBroker;
import static io.bhex.broker.quote.enums.TopicEnum.topN;
import static io.bhex.broker.quote.listener.TopNListener.ALL;
import static io.netty.util.internal.StringUtil.COMMA;

@Slf4j
public class ClientMessageHandler extends SimpleChannelInboundHandler<ClientMessage> {
    private ApplicationContext context;
    private static final String UNDERLINE = "_";
    private SymbolRepository symbolRepository;
    private BhServerRepository bhServerRepository;
    private final static ConcurrentHashMap<Channel, CopyOnWriteArraySet<DataListener>> CHANNEL_LISTENER_MAP = new ConcurrentHashMap<>();

    public ClientMessageHandler(ApplicationContext context) {
        this.context = context;
        this.symbolRepository = context.getBean(SymbolRepository.class);
        this.bhServerRepository = context.getBean(BhServerRepository.class);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ClientMessage clientMessage) throws Exception {

        // event check
        EventEnum eventEnum = EventEnum.valueOF(clientMessage.getEvent());
        if (Objects.isNull(eventEnum)) {
            ctx.channel().writeAndFlush(ErrorCodeEnum.INVALID_EVENT);
            return;
        }

        // 过滤参数，并只保留使用的参数
        filterClientParams(clientMessage);

        // cancel all subscribers
        if (eventEnum == EventEnum.CANCEL_ALL) {
            handleCancelAllEvent(ctx, clientMessage);
            return;
        }

        // topic check
        // 为了兼容之前的kline_1m
        if (StringUtils.isEmpty(clientMessage.getTopic())) {
            ctx.channel().writeAndFlush(ErrorCodeEnum.REQUIRED_TOPIC);
            return;
        }

        String[] topicStrArray = StringUtils.split(clientMessage.getTopic(), UNDERLINE);
        TopicEnum topicEnum = TopicEnum.valueOF(topicStrArray[0]);
        if (topicEnum == null) {
            ctx.channel().writeAndFlush(ErrorCodeEnum.INVALID_TOPIC);
            return;
        }

        if (broker == topicEnum) {
            handleBrokerSubscription(ctx, clientMessage, eventEnum);
            return;
        }

        if (slowBroker == topicEnum) {
            handleSlowBrokerSubscription(ctx, clientMessage, eventEnum);
            return;
        }

        if (topN == topicEnum) {
            handleTopNSubscription(ctx, clientMessage, eventEnum);
            return;
        }

        KlineIntervalEnum klineIntervalEnum = null;
        if (topicEnum == kline || topicEnum == indexKline) {

            if (topicStrArray.length < 2) {
                ctx.channel().writeAndFlush(ErrorCodeEnum.PERIOD_EMPTY);
                return;
            }

            try {
                klineIntervalEnum = KlineIntervalEnum.intervalOf(topicStrArray[1]);
            } catch (IllegalArgumentException e) {
                ctx.channel().writeAndFlush(ErrorCodeEnum.PERIOD_ERROR);
                return;
            }

            Map<String, String> clientMessageParams = clientMessage.getParams();
            clientMessageParams.put(KLINE_TYPE, klineIntervalEnum.interval);
        }

        //  check symbols
        String symbolsListStr = clientMessage.getSymbol();
        if (StringUtils.isEmpty(symbolsListStr)) {
            ctx.channel().writeAndFlush(ErrorCodeEnum.SYMBOLS_ERROR);
            log.info("symbol error: {}", clientMessage);
            return;
        }
        String[] symbolsArray = StringUtils.split(symbolsListStr, COMMA);
        for (String symbols : symbolsArray) {
            String beanName = tryGetBeanName(ctx, clientMessage, symbols);
            if (StringUtils.isEmpty(beanName)) {
                continue;
            }

            if (mergedDepth.equals(topicEnum) || diffMergedDepth.equals(topicEnum)) {
                Map<String, String> clientParams = clientMessage.getParams();
                if (Objects.isNull(clientParams) || !clientParams.containsKey(DUMP_SCALE)) {
                    ctx.channel().writeAndFlush(ErrorCodeEnum.DUMP_SCALE_REQUIRED);
                    return;
                }
                String dumpScale = clientParams.get(DUMP_SCALE);
                // for app bug
                try {
                    int dumpScaleNumber = Integer.parseInt(dumpScale);
                    if (dumpScaleNumber == 0) {
                        dumpScale = "-1";
                    }
                } catch (Exception ignored) {
                }
                beanName = beanName + dumpScale;
            }

            if (indexKline.equals(topicEnum)) {
                KlineIntervalEnum klineInterval = KlineIntervalEnum.intervalOf(clientMessage.getParams().get(KLINE_TYPE));
                beanName = BeanUtils.getIndexKlineBeanName(clientMessage.getSymbol(), klineInterval.interval);
            }

            if (realtimes.equals(topicEnum)) {
                String realtimeInterval = clientMessage.getParams().get(REALTIME_INTERVAL);
                beanName = beanName + DOT + realtimeInterval;
            }

            try {
                DataListener dataListener = (DataListener) context.getBean(beanName);
                handleByEvent(ctx, clientMessage, eventEnum, dataListener);
            } catch (NoSuchBeanDefinitionException ex) {
                ctx.channel().writeAndFlush(ErrorCodeEnum.SYMBOLS_ERROR);
                log.info("Symbols [{}] not exist ex: {}", symbols, ex.getMessage());
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof BizException) {
            ctx.channel().writeAndFlush(((BizException) cause).getErrorCodeEnum());
            return;
        }
        log.warn("ex: ", cause);
        ctx.channel().writeAndFlush(ErrorCodeEnum.SYSTEM_ERROR);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        removeSubscribe(ctx);
    }

    private void handleCancelAllEvent(ChannelHandlerContext ctx, ClientMessage clientMessage) {
        removeSubscribe(ctx);

        ClientMessage response = ClientMessage.builder()
            .cid(clientMessage.getCid())
            .code(ErrorCodeEnum.SUCCESS.getCode())
            .msg(ErrorCodeEnum.SUCCESS.getDesc())
            .symbol(clientMessage.getSymbol())
            .event(clientMessage.getEvent())
            .build();
        ctx.channel().writeAndFlush(response);
    }

    private void handleTopNSubscription(ChannelHandlerContext ctx, ClientMessage clientMessage, EventEnum eventEnum)
        throws BizException {
        long orgId = getOrgId(ctx, clientMessage);
        if (orgId <= 0) {
            ctx.channel().writeAndFlush(ErrorCodeEnum.ORG_ID_INVALID);
            return;
        }
        int type = NumberUtils.toInt(clientMessage.getParams().get(TYPE), ALL);
        String realtimeInterval = clientMessage.getParams().get(REALTIME_INTERVAL);
        try {
            TopNListener topNListener = context.getBean(BeanUtils.getBrokerTopNBeanName(orgId, type, realtimeInterval), TopNListener.class);
            handleByEvent(ctx, clientMessage, eventEnum, topNListener);
        } catch (NoSuchBeanDefinitionException ex) {
            log.warn(ex.getMessage(), ex);
            ctx.channel().writeAndFlush(ErrorCodeEnum.BROKER_NOT_SUPPORT);
        }
    }

    private void handleMarkPriceSubscription(ChannelHandlerContext ctx, ClientMessage clientMessage,
                                             EventEnum eventEnum) {


    }

    private void handleSlowBrokerSubscription(ChannelHandlerContext ctx, ClientMessage clientMessage,
                                              EventEnum eventEnum) throws BizException {
        long orgId = getOrgId(ctx, clientMessage);
        if (orgId <= 0) {
            ctx.channel().writeAndFlush(ErrorCodeEnum.ORG_ID_INVALID);
            return;
        }
        int type = NumberUtils.toInt(clientMessage.getParams().get(TYPE), ALL);
        String realtimeInterval = clientMessage.getParams().get(REALTIME_INTERVAL);
        try {
            SlowBrokerListener slowBrokerListener = context.getBean(BeanUtils.getSlowBrokerBeanName(orgId, type, realtimeInterval),
                SlowBrokerListener.class);
            handleByEvent(ctx, clientMessage, eventEnum, slowBrokerListener);
        } catch (NoSuchBeanDefinitionException ex) {
            log.warn(ex.getMessage(), ex);
            ctx.channel().writeAndFlush(ErrorCodeEnum.BROKER_NOT_SUPPORT);
        }
    }

    private void handleBrokerSubscription(ChannelHandlerContext ctx, ClientMessage clientMessage, EventEnum eventEnum)
        throws BizException {
        long orgId = getOrgId(ctx, clientMessage);
        if (orgId <= 0) {
            ctx.channel().writeAndFlush(ErrorCodeEnum.ORG_ID_INVALID);
            return;
        }
        int type = NumberUtils.toInt(clientMessage.getParams().get(TYPE), ALL);
        String realtimeInterval = clientMessage.getParams().get(REALTIME_INTERVAL);
        try {
            BrokerListener brokerListener = context.getBean(BeanUtils.getBrokerBeanName(orgId, type, realtimeInterval), BrokerListener.class);
            handleByEvent(ctx, clientMessage, eventEnum, brokerListener);
        } catch (NoSuchBeanDefinitionException ex) {
            log.warn(ex.getMessage(), ex);
            ctx.channel().writeAndFlush(ErrorCodeEnum.BROKER_NOT_SUPPORT);
        }
    }

    private long getOrgId(ChannelHandlerContext ctx, ClientMessage clientMessage) {
        Long orgId = (Long) ctx.channel().attr(AttributeKey.valueOf(StringsConstants.ORG_ID)).get();
        orgId = NumberUtils.toLong(clientMessage.getParams().get(ORG), orgId);
        return orgId;
    }

    private void handleByEvent(ChannelHandlerContext ctx, ClientMessage clientMessage, EventEnum eventEnum,
                               DataListener dataListener) {
        switch (eventEnum) {
            case SUB:
                dataListener.addSubscriber(ctx.channel(), clientMessage);
                synchronized (CHANNEL_LISTENER_MAP) {
                    CopyOnWriteArraySet<DataListener> set = CHANNEL_LISTENER_MAP.get(ctx.channel());
                    if (set == null) {
                        set = new CopyOnWriteArraySet<>();
                        CHANNEL_LISTENER_MAP.put(ctx.channel(), set);
                    }
                    set.add(dataListener);
                }
                break;
            case CANCEL:
                dataListener.removeSubscriber(ctx.channel());
                synchronized (CHANNEL_LISTENER_MAP) {
                    CopyOnWriteArraySet<DataListener> removeSet = CHANNEL_LISTENER_MAP.get(ctx.channel());
                    if (removeSet != null) {
                        removeSet.remove(dataListener);
                    }
                }
                break;
            default:
        }
    }

    private void removeSubscribe(ChannelHandlerContext ctx) {
        synchronized (CHANNEL_LISTENER_MAP) {
            CopyOnWriteArraySet<DataListener> set = CHANNEL_LISTENER_MAP.get(ctx.channel());
            if (set != null) {
                for (DataListener dataListener : set) {
                    dataListener.removeSubscriber(ctx.channel());
                }
                CHANNEL_LISTENER_MAP.remove(ctx.channel());
            }
        }
    }

    /**
     * We can get beanName by two methods
     * 1. exchangeId.symbols
     * 2. symbols
     */
    private String tryGetBeanName(ChannelHandlerContext ctx, ClientMessage clientMessage, String symbols) throws BizException {
        String topic = clientMessage.getTopic();
        if (TopicEnum.index.name().equalsIgnoreCase(topic)) {
            return index.name() + UNDERLINE + symbols;
        }
        if (topic.contains(indexKline.name())) {
            return getIndexKlineBeanName(clientMessage, symbols);
        }
        Long orgId = (Long) ctx.channel().attr(AttributeKey.valueOf(ORG_ID)).get();
        if (!StringUtils.contains(symbols, DOT)) {
            try {
                symbols = symbolRepository.getRealSymbolId(orgId, symbols);
                Long exchangeId = symbolRepository.getExchangeIdBySymbolAndOrgId(symbols, orgId);
                Long sharedExchangeId = symbolRepository.getSharedExchangeId(exchangeId, symbols);
                return topic + sharedExchangeId + DOT + symbols;
            } catch (BizException e) {
                ctx.channel().writeAndFlush(e.getErrorCodeEnum());
                return null;
            }
        } else {
            Symbols ss = Symbols.valueOf(symbols);
            String realSymbolId = symbolRepository.getRealSymbolId(orgId, ss.getSymbol());
            Long sharedExchangeId = symbolRepository.getSharedExchangeId(ss.getExchangeId(), realSymbolId);
            return topic + sharedExchangeId + DOT + realSymbolId;
        }
    }

    private String getIndexKlineBeanName(ClientMessage clientMessage, String symbols) throws BizException {
        IndexConfig indexConfig = bhServerRepository.getIndexConfig(symbols);
        if (Objects.isNull(indexConfig)) {
            throw new BizException(ErrorCodeEnum.SYMBOLS_NOT_SUPPORT);
        }
        return BeanUtils.getIndexKlineBeanName(indexConfig.getName(), clientMessage.getParams().get(KLINE_TYPE));
    }

    /**
     * 防止客户端恶意注入
     */
    private void filterClientParams(ClientMessage clientMessage) {
        Map<String, String> clientParams = clientMessage.getParams();
        if (Objects.isNull(clientParams)) {
            //对于老的默认取24小时
            clientParams = new HashMap<>(2);
            clientParams.put(REALTIME_INTERVAL, RealtimeIntervalEnum.H24.getInterval());
            clientMessage.setParams(clientParams);
            return;
        }

        Map<String, String> params = new HashMap<>(8);

        // orgId不会超过5位
        String orgIdStr = clientParams.get(ORG);
        if (Objects.nonNull(orgIdStr)) {
            params.put(ORG, "9001");
        }

        String limitStr = clientParams.get(LIMIT);
        if (Objects.nonNull(limitStr)) {
            // int不会超过11位 -2147483648
            if (limitStr.length() > 15) {
                limitStr = StringUtils.truncate(limitStr, 15);
            }
            params.put(LIMIT, limitStr);
        }

        String klineType = clientParams.get(KLINE_TYPE);
        if (Objects.nonNull(klineType)) {
            if (klineType.length() > 10) {
                klineType = StringUtils.truncate(klineType, 10);
            }
            params.put(KLINE_TYPE, klineType);
        }

        String binary = clientParams.get(BINARY);
        if (Objects.nonNull(binary)) {
            if (binary.length() > 10) {
                binary = StringUtils.truncate(binary, 10);
            }
            params.put(BINARY, binary);
        }

        String dumpScale = clientParams.get(DUMP_SCALE);
        if (Objects.nonNull(dumpScale)) {
            if (dumpScale.length() > 36) {
                dumpScale = StringUtils.truncate(dumpScale, 36);
            }
            params.put(DUMP_SCALE, dumpScale);
        }

        String type = clientParams.get(TYPE);
        if (Objects.nonNull(type)) {
            type = StringUtils.truncate(type, 50);
            params.put(TYPE, type);
        }

        RealtimeIntervalEnum realtimeIntervalEnum = RealtimeIntervalEnum.intervalOf(clientParams.get(REALTIME_INTERVAL));
        params.put(REALTIME_INTERVAL, realtimeIntervalEnum.getInterval());
        clientMessage.setParams(params);

    }
}
