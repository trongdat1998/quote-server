package io.bhex.broker.quote.client;

import io.bhex.base.quote.QuoteRequest;
import io.bhex.broker.quote.bean.ReconnectionInfo;
import io.bhex.broker.quote.bean.Subscription;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

@EqualsAndHashCode(callSuper = true)
@Slf4j
@Data
public class QuoteEngineClient extends Thread {
    private static final AtomicInteger cnt = new AtomicInteger(0);
    private String host;
    private int port;
    private ChannelFuture future;
    private Bootstrap bootstrap;
    private EventLoopGroup worker;
    private String partitionName;
    private QuoteResponseHandler quoteResponseHandler;
    private QuoteInitializeManager manager;
    private boolean reconnect = true;
    private ConcurrentMap<QuoteStreamIndex, Subscription> quoteStreamIndexSubscriptionMap;

    public QuoteEngineClient(String host, int port, String partitionName, ApplicationContext context) {
        super("qs-" + host + "-" + port + "-client" + "-" + cnt.getAndIncrement());
        this.host = host;
        this.port = port;
        this.partitionName = partitionName;
        this.worker = new NioEventLoopGroup();
        this.bootstrap = new Bootstrap();
        this.quoteResponseHandler = new QuoteResponseHandler(host, port, partitionName, context);
        this.quoteStreamIndexSubscriptionMap = new ConcurrentHashMap<>();
        this.manager = context.getBean(QuoteInitializeManager.class);
        bootstrap.group(worker)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
            .option(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator())
            .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(20 * 1024 * 1024, 40 * 1024 * 1024))
            .channel(NioSocketChannel.class)
            .handler(new QuoteEngineInitializer(quoteResponseHandler));
    }

    public void queueSubscription(QuoteStreamIndex quoteStreamIndex) {
        this.quoteResponseHandler.queue(quoteStreamIndex);
    }

    public void sendQuoteRequest(QuoteRequest quoteRequest) {
        quoteResponseHandler.send(quoteRequest);
    }

    public void close(boolean reconnect) {
        try {
            this.reconnect = reconnect;
            future
                .channel()
                .close();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public boolean isActive() {
        return future.channel().isActive();
    }

    @Override
    public void run() {
        try {
            this.future = bootstrap.connect(host, port);
            log.info("connecting to [{}:{}:{}]", partitionName, host, port);
            this.future.sync().channel().closeFuture().sync();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            worker.shutdownGracefully();
            if (reconnect) {
                try {
                    log.info("Reconnect to [{}:{}:{}] in 2s.", partitionName, host, port);
                    Thread.sleep(2000);
                    this.manager.addHeadQuoteEvent(QuoteEvent.builder()
                        .type(QuoteEventTypeEnum.RECONNECT)
                        .data(ReconnectionInfo.builder()
                            .host(host)
                            .port(port)
                            .partitionName(partitionName)
                            .build())
                        .build());
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }

            }
        }
    }
}
