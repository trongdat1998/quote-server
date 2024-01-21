package io.bhex.broker.quote.client;

import com.google.protobuf.InvalidProtocolBufferException;
import io.bhex.base.match.Ticket;
import io.bhex.base.quote.Depth;
import io.bhex.base.quote.KLine;
import io.bhex.base.quote.QuoteResponse;
import io.bhex.base.quote.Realtime;
import io.bhex.broker.quote.client.task.SendBrokerRealTimeTask;
import io.bhex.broker.quote.enums.RealtimeIntervalEnum;
import io.bhex.broker.quote.enums.TopicEnum;
import io.bhex.broker.quote.listener.DepthListener;
import io.bhex.broker.quote.listener.DiffDepthListener;
import io.bhex.broker.quote.listener.DiffMergedDepthListener;
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
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;

import java.util.concurrent.ScheduledExecutorService;

import static io.bhex.broker.quote.enums.TopicEnum.trade;
import static io.bhex.broker.quote.util.ConstantUtil.DEFAULT_MERGE_SCALE;

@Slf4j
public class QuoteStreamObserver implements StreamObserver<QuoteResponse> {
    private Long exchangeId;
    private String symbol;
    private CommandType commandType;
    private ApplicationContext context;
    private QuoteInitializeManager manager;
    private final SymbolRepository symbolRepository;
    private Channel channel;
    private boolean connected = true;
    private volatile boolean autoResubscribe = true;
    private ScheduledExecutorService scheduledExecutorService;
    private static final long INIT_WAIT_TIME = 5_000L;
    private volatile long curWaitTime = INIT_WAIT_TIME;
    // 添加重试之后，此对象生命就线束了
    // 会有新对象来处理
    private volatile boolean isAddedRetry = false;
    //private static final double INC_RATIO = 0.5D;
    //private static final long MAX_WAIT_TIME = 30_000L;
    private DayRealtimeKline dayRealtimeKline;

    public QuoteStreamObserver(Long exchangeId, String symbol, CommandType commandType,
                               Channel channel,
                               ApplicationContext context) {
        this.exchangeId = exchangeId;
        this.symbol = symbol;
        this.commandType = commandType;
        this.channel = channel;
        this.context = context;
        this.scheduledExecutorService = context.getBean("scheduledTaskExecutor", ScheduledExecutorService.class);
        this.manager = context.getBean(QuoteInitializeManager.class);
        this.symbolRepository = context.getBean(SymbolRepository.class);
        this.dayRealtimeKline = context.getBean(DayRealtimeKline.class);
    }

    @Override
    public void onNext(QuoteResponse response) {
        try {
            if (log.isDebugEnabled()) {
                log.debug("[{}:{}:{}] Received msg code:[{}]",
                    exchangeId, symbol, commandType,
                    response.getCode());
            }

            // 订阅失败时
            // 通常是因为quote-engine中还没有支持
            if (CommandType.ERROR.getCode() == response.getCode()) {
                this.connected = false;
                onCompleted();
                return;
            }

            if (CommandType.OK.getCode() == response.getCode()) {
                this.connected = true;
                this.curWaitTime = INIT_WAIT_TIME;
                this.manager.onSubscribed(this.exchangeId, this.symbol, this.commandType);
                return;
            }

            if (CommandType.PONG.getCode() == response.getCode()) {
                //this.manager.onPong(this.exchangeId, this.symbol, this.commandType);
                return;
            }

            // Handler a ticket message
            if (trade.code == response.getCode()) {
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
            else if (TopicEnum.depth.code == response.getCode()) {
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
            else if (TopicEnum.realtimes.code == response.getCode()) {
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
            else if (TopicEnum.kline.code == response.getCode()) {
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
                    dayRealtimeKline.handleDayKline(kLine, kLine.getInterval());
                }
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
    public void onError(Throwable t) {
    }

    @Override
    public void onCompleted() {
    }

    public void setAutoResubscribe(boolean b) {
        this.autoResubscribe = b;
    }


}
