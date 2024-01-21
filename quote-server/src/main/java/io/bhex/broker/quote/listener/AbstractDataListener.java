package io.bhex.broker.quote.listener;

import io.bhex.broker.quote.data.ClientMessage;
import io.bhex.broker.quote.data.dto.ISendData;
import io.bhex.broker.quote.enums.TopicEnum;
import io.bhex.broker.quote.metrics.PushMetrics;
import io.bhex.broker.quote.repository.SymbolRepository;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.ApplicationContext;

import javax.annotation.Resource;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static ch.qos.logback.core.CoreConstants.DOT;
import static io.bhex.broker.quote.enums.StringsConstants.ORG_ID;

/**
 * 公共数据的监听器
 *
 * @author
 */
@Slf4j
@NotThreadSafe
public abstract class AbstractDataListener<RECEIVED, DTO extends ISendData, SNAPSHOT> implements DataListener<RECEIVED> {
    /**
     * 订阅缓存 sessionId -> session
     */
    protected final CopyOnWriteArraySet<Channel> subscriber = new CopyOnWriteArraySet<>();
    Map<Channel, ClientMessage> clientMessageMap = new ConcurrentHashMap<>();
    protected final Class dataTypeClass;

    /**
     * schedule future
     */
    protected ScheduledFuture future;

    /**
     * 从engine来的message
     */
    protected final BlockingQueue<RECEIVED> dataQueue = new LinkedBlockingQueue<>();
    /**
     * 类型 ticker depth ...
     */
    protected TopicEnum topic;
    @Getter
    protected String type;
    /**
     * exchangeId.symbol
     */
    protected String symbols;

    protected String symbol;
    private Long exchangeId;
    /**
     * 从origin数据解析得到，在client第一次连接时发送到client端
     */
    protected SNAPSHOT snapshotData;
    protected volatile long lastTime = -1;

    protected ScheduledExecutorService scheduledTaskExecutor;

    private SymbolRepository symbolRepository;

    public AbstractDataListener(String symbols, TopicEnum topic, Class dataTypeClass, SNAPSHOT snapshot,
                                ApplicationContext context) {
        this.symbols = symbols;
        this.topic = topic;
        this.dataTypeClass = dataTypeClass;
        this.snapshotData = snapshot;
        if (StringUtils.isNotEmpty(symbols)) {
            String[] str = StringUtils.split(symbols, DOT);
            if (str.length < 2) {
                this.exchangeId = 0L;
                this.symbol = str[0];
            } else {
                this.exchangeId = Long.valueOf(str[0]);
                this.symbol = str[1];
            }
        }
        symbolRepository = context.getBean(SymbolRepository.class);
        scheduledTaskExecutor = context.getBean(ScheduledExecutorService.class);
    }

    public AbstractDataListener(String symbols, TopicEnum topic, Class dataTypeClass, ApplicationContext context) {
        this(symbols, topic, dataTypeClass, null, context);
    }

    public void closeAllChannel() {
        for (Channel channel : this.subscriber) {
            channel.close();
        }
    }

    @Override
    public void addSubscriber(Channel channel, ClientMessage clientMessage) {
        clientMessageMap.put(channel, clientMessage);
        subscriber.add(channel);
        ClientMessage.ClientMessageBuilder builder = ClientMessage.builder();
        Long orgId = (Long) channel.attr(AttributeKey.valueOf(ORG_ID)).get();
        String symbolId = symbolRepository.getRealSymbolId(orgId, symbol);
        String symbolName = symbolRepository.getSymbolName(orgId, symbolId);
        builder
            .cid(clientMessage.getCid())
            .symbol(symbolId)
            .symbolName(symbolName)
            .topic(topic.name())
            .f(true)
            .params(clientMessage.getParams());
        if (Objects.isNull(snapshotData)) {
            builder.data(Collections.emptyList());
        } else {
            if (this.snapshotData instanceof Deque
                || this.snapshotData instanceof List) {
                builder.data(handleSnapshot(clientMessage));
            } else {
                builder.data(Collections.singletonList(handleSnapshot(clientMessage)));
            }
        }
        channel.writeAndFlush(builder
            .sendTime(System.currentTimeMillis())
            .build());

        PushMetrics.addSubscriber(this.topic.name());
    }

    protected abstract SNAPSHOT handleSnapshot(ClientMessage clientMessage);

    @Override
    public void removeSubscriber(Channel channel) {
        subscriber.remove(channel);
        clientMessageMap.remove(channel);

        PushMetrics.removeSubscriber(this.topic.name());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onEngineMessage(RECEIVED data) {
        dataQueue.add(data);
    }

    /**
     * 处理消息
     * 更新快照数据，在需要个性化快照数据时，要重写此方法
     * 如果返回为NULL ，说明要放弃这条数据
     *
     * @param message message
     * @return snapshot
     */
    protected abstract DTO onMessage(RECEIVED message);

    /**
     * 更新缓存信息
     *
     * @param dto dto
     */
    protected abstract DTO buildSnapshot(DTO dto);

    protected abstract void metric(RECEIVED dto);

    public SNAPSHOT getSnapshotData() {
        return snapshotData;
    }

    @Override
    public void watching() {
        this.future = scheduledTaskExecutor.schedule(() -> {
            try {
                this.scheduleTask();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }, 1000, TimeUnit.MILLISECONDS);
    }

    private void scheduleTask() {
        try {
            consumeData();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            this.future = scheduledTaskExecutor.schedule(this::scheduleTask, 300, TimeUnit.MILLISECONDS);
        }
    }

    private void consumeData() {
        try {
            while (!dataQueue.isEmpty()) {
                List<RECEIVED> dataList = collectReceivedData();

                List<DTO> responseDataList = handleAndBuildSnapshot(dataList);

                if (CollectionUtils.isNotEmpty(responseDataList)) {
                    sendData(responseDataList);
                    metric(dataList.get(dataList.size() - 1));
                }
            }
        } catch (Exception e) {
            log.error("Send msg ex: ", e);
        }
    }

    private List<RECEIVED> collectReceivedData() {
        List<RECEIVED> dataList = new LinkedList<>();
        RECEIVED received;
        int size = dataQueue.size();
        while (size-- > 0 && (received = dataQueue.poll()) != null) {
            dataList.add(received);
        }
        return dataList;
    }

    private List<DTO> handleAndBuildSnapshot(List<RECEIVED> dataList) {
        List<DTO> responseDataList = new ArrayList<>();
        for (RECEIVED t : dataList) {
            DTO dto = onMessage(t);
            if (Objects.nonNull(dto)) {
                dto = buildSnapshot(dto);
                if (dto.getCurId() >= lastTime) {
                    lastTime = dto.getCurId();
                    responseDataList.add(dto);
                }
            }
        }
        return responseDataList;
    }

    private void sendData(List<DTO> responseDataList) {
        List<DTO> data = Collections.unmodifiableList(responseDataList);
        long sendTime = System.currentTimeMillis();
        clientMessageMap.forEach((channel, clientMessage) -> {
            Long orgId = (Long) channel.attr(AttributeKey.valueOf(ORG_ID)).get();
            String symbolId = symbolRepository.getRealSymbolId(orgId, symbol);
            String symbolName = symbolRepository.getSymbolName(orgId, symbolId);

            ClientMessage msg = ClientMessage.builder()
                .cid(clientMessage.getCid())
                .symbol(symbolId)
                .symbolName(symbolName)
                .topic(topic.name())
                .data(data)
                .params(clientMessage.getParams())
                .sendTime(sendTime)
                .f(false)
                .build();
            channel.writeAndFlush(msg);
        });
    }

    @Override
    public TopicEnum getTopic() {
        return topic;
    }

    @Override
    public String getSymbols() {
        return symbols;
    }

    @Override
    public String getSymbol() {
        return symbol;
    }

    @Override
    public Long getExchangeId() {
        return exchangeId;
    }

    @Override
    public void stop() {
        if (!this.future.isCancelled()) {
            this.future.cancel(true);
        }
    }
}
