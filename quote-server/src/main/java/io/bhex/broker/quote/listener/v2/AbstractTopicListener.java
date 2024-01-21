package io.bhex.broker.quote.listener.v2;

import io.bhex.broker.quote.data.ClientMessageV2;
import io.bhex.broker.quote.data.dto.IEngineData;
import io.bhex.broker.quote.enums.ErrorCodeEnum;
import io.bhex.broker.quote.metrics.PushMetrics;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Scheduler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static reactor.core.publisher.FluxSink.OverflowStrategy.DROP;

@Slf4j
public abstract class AbstractTopicListener<T, DTO extends IEngineData> implements ITopicListener<T> {
    private ConcurrentMap<Channel, ClientMessageV2> subscribers = new ConcurrentHashMap<>();
    private UnicastProcessor<T> depthUnicastProcessor;
    private FluxSink<T> depthFlux;
    private ApplicationContext context;
    private Scheduler scheduler;

    protected AbstractTopicListener(ApplicationContext context) {
        this.context = context;
        this.depthUnicastProcessor = UnicastProcessor.create();
        this.depthFlux = this.depthUnicastProcessor.sink(DROP);
        this.scheduler = this.context.getBean("push-scheduler", Scheduler.class);
    }

    @Override
    public void subscribe(Channel channel, ClientMessageV2 msg) {
        channel.writeAndFlush(ClientMessageV2
            .builder()
            .event(msg.getEvent())
            .topic(msg.getTopic())
            .params(msg.getParams())
            .code(ErrorCodeEnum.SUCCESS.getCode())
            .msg(ErrorCodeEnum.SUCCESS.getDesc())
            .build());
        subscribers.put(channel, msg);
    }

    @Override
    public void unsubscribe(Channel channel, ClientMessageV2 msg) {
        subscribers.remove(channel);
        channel.writeAndFlush(ClientMessageV2
            .builder()
            .event(msg.getEvent())
            .topic(msg.getTopic())
            .params(msg.getParams())
            .code(ErrorCodeEnum.SUCCESS.getCode())
            .msg(ErrorCodeEnum.SUCCESS.getDesc())
            .build());
    }

    @Override
    public void onMessage(T data) {
        this.depthFlux.next(data);
    }

    public void watch() {
        this.depthUnicastProcessor
            .publishOn(this.scheduler)
            .map(this::convertFrom)
            .doOnNext(this::sendData)
            .doOnError(this::onError)
            .subscribe();
    }

    protected abstract DTO convertFrom(T data);

    private void sendData(DTO dto) {
        this.subscribers
                .entrySet()
                .parallelStream()
                .forEach((c) -> {
                    try {
                        sendData(c.getKey(), c.getValue(), dto);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            );
    }

    private void sendData(Channel c, ClientMessageV2 m, DTO dto) {
        ClientMessageV2 msg = ClientMessageV2.builder()
            .topic(getTopic().name())
            .data(dto)
            .params(m.getParams())
            .build();
        PushMetrics.pushedMessageLatencyV2(getTopic().name(), System.currentTimeMillis() - dto.getTime());
        c.writeAndFlush(msg);
    }

    protected void onError(Throwable e) {
        log.error(e.getMessage(), e);
    }

}
