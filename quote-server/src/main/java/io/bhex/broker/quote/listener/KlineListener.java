package io.bhex.broker.quote.listener;

import io.bhex.base.quote.KLine;
import io.bhex.broker.quote.data.ClientMessage;
import io.bhex.broker.quote.data.dto.KlineItemDTO;
import io.bhex.broker.quote.enums.TopicEnum;
import io.bhex.broker.quote.metrics.PushMetrics;
import io.bhex.broker.quote.repository.SymbolRepository;
import io.bhex.broker.quote.util.FilterHistoryKlineUtil;
import io.bhex.exchange.enums.KlineIntervalEnum;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.context.ApplicationContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.bhex.broker.quote.enums.StringsConstants.LIMIT;
import static io.bhex.broker.quote.enums.StringsConstants.ORG_ID;

@Slf4j
public class KlineListener extends AbstractDataListener<KLine, KlineItemDTO, ConcurrentLinkedDeque<KlineItemDTO>> {
    private SymbolRepository symbolRepository;

    public KlineListener(String symbol, String interval, TopicEnum topic, Class dataTypeClass, ApplicationContext context) {
        super(symbol, topic, dataTypeClass, new ConcurrentLinkedDeque<>(), context);
        this.interval = interval;
        this.symbolRepository = context.getBean(SymbolRepository.class);
    }

    /**
     * 用于返回KLine类型，而不是DTO的类型，DTO类型存在snapshotData中
     * 原始快照数据按时间顺序，从小到大排序
     * 在http请求时，需要用此数据
     */
    @Getter
    private ConcurrentLinkedDeque<KLine> originSnapshotData = new ConcurrentLinkedDeque<>();

    @Getter
    private String interval;

    public static final int KLINE_CACHE_SIZE = 3500;

    @Override
    public KlineItemDTO onMessage(KLine message) {
        buildOriginSnapshotData(message);
        PushMetrics.receivedMessageFromGRpc(message.getExchangeId(), this.topic.name(), System.currentTimeMillis() - message.getId());
        if (KlineIntervalEnum.M1.interval.equals(interval)) {
            PushMetrics.markM1KlineLastTime(message.getExchangeId(), message.getSymbol(), message.getId());
        }
        String symbolName = symbolRepository.getSymbolNameByBrokerSymbol(message.getSymbol());
        return KlineItemDTO.parse(message, symbolName);
    }

    private void buildOriginSnapshotData(KLine message) {
        if (CollectionUtils.isNotEmpty(originSnapshotData) && message.getId() <= originSnapshotData.getLast().getId()) {
            Iterator<KLine> it = originSnapshotData.descendingIterator();
            LinkedList<KLine> originSnapshotData = new LinkedList<>();
            while (it.hasNext()) {
                KLine kLine = it.next();
                if (kLine.getId() == message.getId()) {
                    originSnapshotData.addFirst(message);
                } else {
                    originSnapshotData.addFirst(kLine);
                }
            }
            this.originSnapshotData = new ConcurrentLinkedDeque<>(originSnapshotData);
        } else {
            this.originSnapshotData.addLast(message);
        }

        if (originSnapshotData.size() > KLINE_CACHE_SIZE) {
            originSnapshotData.removeFirst();
        }
    }

    /**
     * 快照数据按时间顺序从小到大排序
     */
    @Override
    public KlineItemDTO buildSnapshot(KlineItemDTO klineItemDTO) {
        if (CollectionUtils.isNotEmpty(snapshotData) && klineItemDTO.getTime() <= snapshotData.getLast().getTime()) {
            Iterator<KlineItemDTO> it = snapshotData.descendingIterator();
            while (it.hasNext()) {
                KlineItemDTO itemDTO = it.next();
                if (itemDTO.getTime() == klineItemDTO.getTime()) {
                    itemDTO.cloneData(klineItemDTO);
                }
            }
        } else {
            snapshotData.addLast(klineItemDTO);
        }
        if (snapshotData.size() > KLINE_CACHE_SIZE) {
            snapshotData.removeFirst();
        }
        return klineItemDTO;
    }

    @Override
    protected void metric(KLine dto) {
        PushMetrics.pushedMessageLatency(dto.getExchangeId(), topic.name(), System.currentTimeMillis() - dto.getId());
    }

    @Override
    public void addSubscriber(Channel channel, ClientMessage clientMessage) {
        clientMessageMap.put(channel, clientMessage);
        subscriber.add(channel);
        Long orgId = (Long) channel.attr(AttributeKey.valueOf(ORG_ID)).get();
        String symbolId = symbolRepository.getRealSymbolId(orgId, symbol);
        String symbolName = symbolRepository.getSymbolName(orgId, symbolId);
        ClientMessage.ClientMessageBuilder builder = ClientMessage.builder()
            .cid(clientMessage.getCid())
            .symbol(symbolId)
            .symbolName(symbolName)
            .topic(topic.name())
            .f(true)
            .params(clientMessage.getParams());
        if (Objects.isNull(snapshotData)) {
            builder.data(Collections.emptyList());
        } else {
            ConcurrentLinkedDeque<KlineItemDTO> originalSnapshot = handleSnapshot(clientMessage);
            long filterKlineStartId = FilterHistoryKlineUtil.getInstance().getFilterHistoryKlineTimeStartId(orgId, symbolId, KlineIntervalEnum.intervalOf(interval));
            if (filterKlineStartId > 0) {
                if (originalSnapshot.isEmpty()) {
                    //未知,存储filterShareKlineTime
                    builder.data(originalSnapshot);
                } else if (originalSnapshot.getFirst().getCurId() >= filterKlineStartId) {
                    builder.data(originalSnapshot);
                } else if (originalSnapshot.getLast().getCurId() < filterKlineStartId) {
                    builder.data(Collections.emptyList());
                } else {
                    //截取
                    while (originalSnapshot.getFirst().getCurId() < filterKlineStartId) {
                        originalSnapshot.removeFirst();
                    }
                    builder.data(originalSnapshot);
                }
            } else {
                builder.data(originalSnapshot);
            }
        }
        channel.writeAndFlush(builder
            .sendTime(System.currentTimeMillis())
            .build());
        PushMetrics.addSubscriber(this.topic.name());
    }

    @Override
    protected ConcurrentLinkedDeque<KlineItemDTO> handleSnapshot(ClientMessage clientMessage) {
        Map<String, String> params = clientMessage.getParams();
        int limit = 1;
        if (Objects.nonNull(params)) {
            limit = NumberUtils.toInt(params.get(LIMIT), 1);
        }
        ConcurrentLinkedDeque<KlineItemDTO> resultList = new ConcurrentLinkedDeque<>();
        int size = snapshotData.size();
        if (limit > size) {
            Iterator<KlineItemDTO> klineItemDTOIterator = snapshotData.iterator();
            for (int i = 0; i < size; ++i) {
                if (klineItemDTOIterator.hasNext()) {
                    resultList.addLast(klineItemDTOIterator.next());
                } else {
                    break;
                }
            }
            return resultList;
        } else {
            Iterator<KlineItemDTO> it = snapshotData.descendingIterator();
            for (int i = 0; i < limit; i++) {
                if (it.hasNext()) {
                    resultList.addFirst(it.next());
                } else {
                    break;
                }
            }
            return resultList;
        }
    }

    @Override
    public void clearSnapshot() {
        snapshotData.clear();
    }

    @Override
    public void watching() {
        this.future = scheduledTaskExecutor.schedule(() -> {
            try {
                scheduleTask();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }, 1000, TimeUnit.MILLISECONDS);
    }

    private void scheduleTask() {
        try {
            consumeData();
        } catch (Exception e) {
            log.error("Send msg ex: ", e);
        } finally {
            this.future = scheduledTaskExecutor.schedule(this::scheduleTask, 300, TimeUnit.MILLISECONDS);
        }
    }

    private void consumeData() {
        while (!dataQueue.isEmpty()) {
            KLine kline = dataQueue.poll();
            if (Objects.isNull(kline)) {
                continue;
            }

            List<KlineItemDTO> responseDataList = new ArrayList<>();
            KlineItemDTO dto = onMessage(kline);
            if (Objects.nonNull(dto)) {
                dto = buildSnapshot(dto);
                if (dto.getCurId() >= lastTime) {
                    lastTime = dto.getCurId();
                    responseDataList.add(dto);
                }
            }

            if (CollectionUtils.isNotEmpty(responseDataList)) {
                List<KlineItemDTO> data = Collections.unmodifiableList(responseDataList);
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
                        .f(false)
                        .sendTime(sendTime)
                        .build();
                    channel.writeAndFlush(msg);
                });

                metric(kline);
            }
        }
    }

    public void reloadHistoryData(List<KLine> kLineList) {
        kLineList = filteredKline(kLineList);
        this.originSnapshotData = new ConcurrentLinkedDeque<>(kLineList);
        this.snapshotData = new ConcurrentLinkedDeque<>(getKlineItemDTOList(kLineList));
        if (CollectionUtils.isNotEmpty(this.snapshotData)) {
            this.lastTime = this.snapshotData.getLast().getCurId();
        }
        dataQueue.clear();
    }

    private List<KLine> filteredKline(List<KLine> kLineList) {
        List<KLine> filteredKlineList = new ArrayList<>();
        List<KLine> tmpList = new ArrayList<>();
        tmpList.addAll(kLineList);
        tmpList.sort(Comparator.comparingLong(KLine::getId));
        long lastTime = 0;
        for (KLine kLine : tmpList) {
            if (kLine.getId() != lastTime) {
                filteredKlineList.add(kLine);
                lastTime = kLine.getId();
            }
        }
        return filteredKlineList;
    }

    private List<KlineItemDTO> getKlineItemDTOList(List<KLine> kLineList) {
        return kLineList.stream()
            .map(data -> {
                String symbolName = symbolRepository.getSymbolNameByBrokerSymbol(data.getSymbol());
                return KlineItemDTO.parse(data, symbolName);
            })
            .collect(Collectors.toList());
    }
}
