package io.bhex.broker.quote.client;

import com.google.protobuf.InvalidProtocolBufferException;
import io.bhex.base.match.Ticket;
import io.bhex.base.quote.Depth;
import io.bhex.base.quote.KLine;
import io.bhex.base.quote.QuoteRequest;
import io.bhex.base.quote.QuoteResponse;
import io.bhex.base.quote.Realtime;
import io.bhex.base.quote.Subscription;
import io.bhex.broker.quote.client.task.SendBrokerRealTimeTask;
import io.bhex.broker.quote.enums.RealtimeIntervalEnum;
import io.bhex.broker.quote.enums.TopicEnum;
import io.bhex.broker.quote.listener.DepthListener;
import io.bhex.broker.quote.listener.DiffDepthListener;
import io.bhex.broker.quote.listener.DiffMergedDepthListener;
import io.bhex.broker.quote.listener.IndexKlineListener;
import io.bhex.broker.quote.listener.KlineListener;
import io.bhex.broker.quote.listener.MergedDepthListener;
import io.bhex.broker.quote.listener.RealTimeListener;
import io.bhex.broker.quote.listener.TradeListener;
import io.bhex.broker.quote.listener.v2.BookTickerListener;
import io.bhex.broker.quote.listener.v2.TicketListener;
import io.bhex.broker.quote.metrics.PushMetrics;
import io.bhex.broker.quote.repository.SymbolRepository;
import io.bhex.broker.quote.util.BeanUtils;
import io.bhex.ex.quote.core.enums.CommandType;
import io.bhex.exchange.enums.KlineIntervalEnum;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;

import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.bhex.broker.quote.enums.TopicEnum.trade;
import static io.bhex.broker.quote.util.ConstantUtil.DEFAULT_MERGE_SCALE;

@Slf4j
public class QuoteResponseHandler extends SimpleChannelInboundHandler<QuoteResponse> {

    private ChannelHandlerContext ctx;

    private ApplicationContext context;
    private SymbolRepository symbolRepository;
    private ScheduledExecutorService scheduledExecutorService;
    private QuoteInitializeManager manager;

    private AtomicInteger hbCnt;
    private Future hbFuture;
    private static final int HB_THRESHOLD = 10;
    private String partitionName;
    private String host;
    private int port;
    private Deque<QuoteRequest> pendingSendSubscription;
    private DayRealtimeKline dayRealtimeKline;

    public QuoteResponseHandler(String host, int port, String partitionName, ApplicationContext context) {
        this.context = context;
        this.scheduledExecutorService = context.getBean("scheduledTaskExecutor", ScheduledExecutorService.class);
        this.manager = context.getBean(QuoteInitializeManager.class);
        this.symbolRepository = context.getBean(SymbolRepository.class);
        this.dayRealtimeKline = context.getBean(DayRealtimeKline.class);
        this.partitionName = partitionName;
        this.pendingSendSubscription = new LinkedList<>();
        this.host = host;
        this.port = port;
    }

    public void queue(QuoteStreamIndex quoteStreamIndex) {
        long time = System.currentTimeMillis();
        QuoteRequest quoteRequest = QuoteRequest.newBuilder()
            .setExchangeId(quoteStreamIndex.getExchangeId())
            .setSymbol(quoteStreamIndex.getSymbol())
            .setType(quoteStreamIndex.getCommandType().getCode())
            .setSub(true)
            .setTime(time)
            .build();
        pendingSendSubscription.add(quoteRequest);
    }

    // 如果还没有连接成功，就要发送订阅，添加到队列中，连接成功后会先从queue中执行消息
    public void send(QuoteRequest quoteRequest) {
        if (this.ctx == null || !ctx.channel().isActive()) {
            this.pendingSendSubscription.addLast(quoteRequest);
            return;
        }
        if (ctx.channel().isWritable()) {
            ctx.channel().writeAndFlush(quoteRequest);
        } else {
            this.pendingSendSubscription.addLast(quoteRequest);
            log.warn("write buffer full");
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        super.channelWritabilityChanged(ctx);
        if (ctx.channel().isWritable()) {
            while (!pendingSendSubscription.isEmpty()) {
                send(pendingSendSubscription.removeFirst());
            }
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("Connected to [{}:{}] status [{}]", host, port, ctx.channel().isActive());
        this.ctx = ctx;
        while (!pendingSendSubscription.isEmpty()) {
            QuoteRequest quoteRequest = pendingSendSubscription.removeFirst();
            send(quoteRequest);
        }
        this.hbCnt = new AtomicInteger(0);
        this.hbFuture = this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                log.info("pendingSendSubscription queue size [{}]", pendingSendSubscription.size());

                int curHbCnt = hbCnt.incrementAndGet();
                if (curHbCnt >= HB_THRESHOLD) {
                    log.info("Lost hb to [{}:{}:{}] will reconnect to.", partitionName, host, port);
                    reconnect();
                    return;
                }
                log.info("Ping [{}:{}:{}] cnt [{}] ", partitionName, host, port, curHbCnt);
                send(QuoteRequest.newBuilder()
                    .setType(CommandType.PING.getCode())
                    .build());
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    public void reconnect() {
        this.ctx.channel().close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("Lost connection to [{}:{}]", host, port);
        // 关闭心跳
        try {
            this.hbFuture.cancel(true);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        super.channelInactive(ctx);
    }

    public void handle(QuoteResponse response) {
        try {
            long exchangeId = response.getExchangeId();
            String symbol = response.getSymbol();
            if (log.isDebugEnabled()) {
                log.debug("[{}:{}] Received msg code:[{}]",
                    response.getExchangeId(), response.getSymbol(), response.getCode());
            }

            // 订阅失败时
            // 通常是因为quote-engine中还没有支持
            if (CommandType.ERROR.getCode() == response.getCode()) {
                return;
            }

            if (CommandType.OK.getCode() == response.getCode()) {
                Subscription subscription = Subscription.parseFrom(response.getMessage());
                CommandType cmd = CommandType.codeOf(subscription.getType());
                if (subscription.getSub()) {
                    log.info("Client rcv msg subscribed [{}:{}:{}]", subscription.getExchangeId(),
                        subscription.getSymbolId(), cmd);
                    this.manager.markSubscribed(partitionName, exchangeId, symbol, cmd);
                } else {
                    log.info("Client rcv msg unsubscribed [{}:{}:{}]", subscription.getExchangeId(),
                        subscription.getSymbolId(), cmd);
                    this.manager.markUnsubscribed(partitionName, exchangeId, symbol, cmd);
                }
                return;
            }

            if (CommandType.PONG.getCode() == response.getCode()) {
                log.info("Received pong from [{}:{}:{}]", partitionName, host, port);
                this.hbCnt.set(0);
                return;
            }

            // Handler a ticket message
            if (CommandType.TICKET.getCode() == response.getCode()) {
                Ticket ticket = Ticket.parseFrom(response.getMessage());
                PushMetrics.recordQuoteEngineEvent(ticket.getExchangeId(), ticket.getSymbolId(), trade.name(), response.getTime());
                TradeListener tradeListener = (TradeListener) context
                    .getBean(BeanUtils.getTradesBeanName(ticket.getExchangeId(), ticket.getSymbolId()));
                tradeListener.onEngineMessage(ticket);

                TicketListener ticketListener = context.getBean(BeanUtils
                        .getTradeV2BeanName(ticket.getExchangeId(), ticket.getSymbolId()),
                    TicketListener.class);
                ticketListener.onMessage(ticket);
            }

            // Handle depth message
            else if (CommandType.DEPTH.getCode() == response.getCode()) {
                Depth depth = Depth.parseFrom(response.getMessage());
                PushMetrics.recordQuoteEngineEvent(depth.getExchangeId(), depth.getSymbol(), TopicEnum.depth.name(), response.getTime());
                if (depth.getDumpScale() == DEFAULT_MERGE_SCALE) {
                    String depthName = BeanUtils.getDepthBeanName(depth.getExchangeId(), depth.getSymbol());
                    String diffDepthName = BeanUtils.getDiffDepthBeanName(depth.getExchangeId(), depth.getSymbol());
                    DepthListener depthListener = (DepthListener) context.getBean(depthName);
                    DiffDepthListener diffDepthListener = (DiffDepthListener) context
                        .getBean(diffDepthName);
                    depthListener.onEngineMessage(depth);
                    diffDepthListener.onEngineMessage(depth);
                }
                try {
                    int maxScale = symbolRepository.getMaxScale(depth.getSymbol());
                    if (depth.getDumpScale() == maxScale) {
                        io.bhex.broker.quote.listener.v2.DepthListener depthListener = context
                            .getBean(BeanUtils.getDepthV2BeanName(depth.getExchangeId(), depth.getSymbol()),
                                io.bhex.broker.quote.listener.v2.DepthListener.class);
                        depthListener.onMessage(depth);
                        BookTickerListener bookTickerListener = context
                            .getBean(BeanUtils.getBookTickerV2BeanName(depth.getExchangeId(), depth.getSymbol()),
                                BookTickerListener.class);
                        bookTickerListener.onMessage(depth);
                    }
                } catch (Exception e) {
                    log.warn(e.getMessage(), e);
                }

                try {
                    String mergedDepthName = BeanUtils.getDepthBeanName(depth.getExchangeId(), depth.getSymbol(),
                        depth.getDumpScale());
                    String diffMergedDepthName = BeanUtils.getDiffDepthBeanName(depth.getExchangeId(), depth.getSymbol(),
                        depth.getDumpScale());
                    MergedDepthListener mergedDepthListener = (MergedDepthListener) context.getBean(mergedDepthName);
                    DiffMergedDepthListener diffMergedDepthListener = (DiffMergedDepthListener) context
                        .getBean(diffMergedDepthName);
                    mergedDepthListener.onEngineMessage(depth);
                    diffMergedDepthListener.onEngineMessage(depth);
                } catch (NoSuchBeanDefinitionException e) {
                    log.warn(e.getMessage(), e);
                    this.manager.registerMergedDepth(exchangeId, symbol, depth.getDumpScale());
                }
            }

            // Handler a RealTimeDTO message
            else if (CommandType.REAL_TIME.getCode() == response.getCode()) {
                Realtime realtime = Realtime.parseFrom(response.getMessage());
                PushMetrics.recordQuoteEngineEvent(realtime.getExchangeId(), realtime.getS(), TopicEnum.realtimes.name(), response.getTime());
                RealTimeListener realTimeListener = (RealTimeListener) context
                        .getBean(BeanUtils.getRealTimeBeanName(realtime.getExchangeId(), realtime.getS(), RealtimeIntervalEnum.H24.getInterval()));
                realTimeListener.onEngineMessage(realtime);

                scheduledExecutorService.execute(new SendBrokerRealTimeTask(symbolRepository, context, realtime));

                io.bhex.broker.quote.listener.v2.RealTimeListener realTimeListenerV2 =
                        context.getBean(BeanUtils.getTickerV2BeanName(realtime.getExchangeId(), realtime.getS(), RealtimeIntervalEnum.H24.getInterval()),
                                io.bhex.broker.quote.listener.v2.RealTimeListener.class);
                realTimeListenerV2.onMessage(realtime);
            }

            // Handler a RealTimeKLine message
            else if (CommandType.KLINE.getCode() == response.getCode()) {
                KLine kLine = KLine.parseFrom(response.getMessage());
                PushMetrics.recordQuoteEngineEvent(kLine.getExchangeId(), kLine.getSymbol(), TopicEnum.kline.name(), response.getTime());
                KlineListener klineListener = (KlineListener) context
                    .getBean(BeanUtils.getKlineBeanName(kLine.getExchangeId(), kLine.getSymbol(), kLine.getInterval()));
                klineListener.onEngineMessage(kLine);

                io.bhex.broker.quote.listener.v2.KlineListener klineListenerV2 = context.getBean(BeanUtils
                        .getKlineV2BeanName(kLine.getExchangeId(), kLine.getSymbol(), kLine.getInterval()),
                    io.bhex.broker.quote.listener.v2.KlineListener.class);
                klineListenerV2.onMessage(kLine);

                //处理日线相关的数据
                if (KlineIntervalEnum.D1.interval.equals(kLine.getInterval()) || KlineIntervalEnum.D1_8.interval.equals(kLine.getInterval())) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}:{}] Received day kline:[{}]", response.getExchangeId(), response.getSymbol(), kLine.getInterval());
                    }
                    dayRealtimeKline.handleDayKline(kLine, kLine.getInterval());
                }
            }

            // Index kline
            else if (CommandType.INDEX_KLINE.getCode() == response.getCode()) {
                KLine kLine = KLine.parseFrom(response.getMessage());
                PushMetrics.recordQuoteEngineEvent(kLine.getExchangeId(), kLine.getSymbol(), TopicEnum.indexKline.name(), response.getTime());
                IndexKlineListener indexKlineListener = (IndexKlineListener) context
                    .getBean(BeanUtils.getIndexKlineBeanName(kLine.getSymbol(), kLine.getInterval()));
                indexKlineListener.onEngineMessage(kLine);

            }

            // Unknown data
            else {
                log.warn("Unknown data type [{}] exchangeId [{}] symbol [{}] message [{}]",
                    response.getCode(), response.getExchangeId(), response.getSymbol(), response.getMessage());
            }
        } catch (NoSuchBeanDefinitionException e) {
            log.warn(e.getMessage());
        } catch (InvalidProtocolBufferException e) {
            log.error("Parse error code [{}] message [{}]", response.getCode(), response.getMessage(), e);
        }

    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, QuoteResponse msg) throws Exception {
        handle(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error(cause.getMessage(), cause);
    }
}
