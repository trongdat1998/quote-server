package io.bhex.broker.quote.listener;

import io.bhex.broker.quote.data.ClientMessage;
import io.bhex.broker.quote.data.dto.IEngineData;
import io.bhex.broker.quote.enums.TopicEnum;
import io.bhex.broker.quote.metrics.PushMetrics;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import reactor.core.Disposable;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Scheduler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static reactor.core.publisher.FluxSink.OverflowStrategy.DROP;

@Slf4j
public abstract class AbstractDataListenerV2<RECEIVED, DTO extends IEngineData, SNAPSHOT> implements DataListener<RECEIVED> {
    private ConcurrentMap<Channel, ClientMessage> subscribers = new ConcurrentHashMap<>();
    private UnicastProcessor<RECEIVED> depthUnicastProcessor;
    private FluxSink<RECEIVED> depthFlux;
    private ApplicationContext context;
    protected Scheduler scheduler;
    protected SNAPSHOT snapshot;
    protected TopicEnum topicEnum;
    protected Disposable disposable;

    public AbstractDataListenerV2(TopicEnum topicEnum, ApplicationContext context) {
        this.topicEnum = topicEnum;
        this.context = context;
        this.depthUnicastProcessor = UnicastProcessor.create();
        this.depthFlux = this.depthUnicastProcessor.sink(DROP);
        this.scheduler = this.context.getBean("push-scheduler", Scheduler.class);
    }

    @Override
    public void onEngineMessage(RECEIVED data) {
        this.depthFlux.next(data);
    }

    protected abstract SNAPSHOT handleSnapshot(ClientMessage clientMessage);

    @Override
    public void removeSubscriber(Channel channel) {
        subscribers.remove(channel);
        PushMetrics.removeSubscriber(this.topicEnum.name());
    }


    @Override
    public void addSubscriber(Channel channel, ClientMessage clientMessage) {
        subscribers.put(channel, clientMessage);
        PushMetrics.addSubscriber(this.topicEnum.name());
    }

    @Override
    public void watching() {
        this.disposable = this.depthUnicastProcessor
            .publishOn(this.scheduler)
            .map(this::convertFrom)
            .doOnNext(this::sendData)
            .doOnError(this::onError)
            .subscribe();
    }

    protected abstract DTO convertFrom(RECEIVED data);

    protected abstract SNAPSHOT buildSnapshot(DTO dto);

    private void sendData(DTO dto) {
        this.snapshot = buildSnapshot(dto);
        long sendTime = System.currentTimeMillis();
        this.subscribers
            .forEach((c, m) -> {
                    try {
                        sendData(c, m, dto, sendTime);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            );
    }

    private void sendData(Channel c, ClientMessage m, DTO dto, long sendTime) {
        ClientMessage msg = ClientMessage.builder()
            .topic(getTopic().name())
            .data(dto)
            .params(m.getParams())
            .sendTime(sendTime)
            .build();
        c.writeAndFlush(msg);
        PushMetrics.pushedMessageLatency(getTopic().name(), System.currentTimeMillis() - dto.getTime());
    }

    public void onError(Throwable e) {
        log.error(e.getMessage(), e);
    }

    @Override
    public TopicEnum getTopic() {
        return this.topicEnum;
    }

    @Override
    public void clearSnapshot() {
        this.snapshot = null;
    }

    @Override
    public void stop() {
        this.disposable.dispose();
    }
}
