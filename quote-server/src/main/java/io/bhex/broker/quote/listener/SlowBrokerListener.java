package io.bhex.broker.quote.listener;

import io.bhex.base.quote.Realtime;
import io.bhex.broker.grpc.basic.Symbol;
import io.bhex.broker.quote.data.ClientMessage;
import io.bhex.broker.quote.data.QuoteIndex;
import io.bhex.broker.quote.data.dto.RealTimeDTO;
import io.bhex.broker.quote.enums.TopicEnum;
import io.bhex.broker.quote.metrics.PushMetrics;
import io.bhex.broker.quote.repository.SymbolRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.ApplicationContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.bhex.broker.quote.enums.TopicEnum.broker;
import static io.bhex.broker.quote.listener.TopNListener.ALL;

@Slf4j
public class SlowBrokerListener extends AbstractDataListener<Realtime, RealTimeDTO, LinkedList<RealTimeDTO>> {
    private Long orgId;
    private ConcurrentMap<QuoteIndex, RealTimeDTO> realTimeMap = new ConcurrentHashMap<>();
    // 等待被发送的数据
    private final ConcurrentMap<QuoteIndex, RealTimeDTO> pendingMap = new ConcurrentHashMap<>();

    private final int symbolType;
    private final SymbolRepository symbolRepository;

    public SlowBrokerListener(Long orgId, TopicEnum topic, Class dataTypeClass,
                              ScheduledExecutorService scheduledExecutorService,
                              int symbolType,
                              ApplicationContext context) {
        super(StringUtils.EMPTY, topic, dataTypeClass, new LinkedList<>(), context);
        this.orgId = orgId;
        this.scheduledTaskExecutor = scheduledExecutorService;
        this.symbolType = symbolType;
        this.symbolRepository = context.getBean(SymbolRepository.class);
    }

    @Override
    protected LinkedList<RealTimeDTO> handleSnapshot(ClientMessage clientMessage) {
        return this.snapshotData;
    }

    @Override
    protected RealTimeDTO onMessage(Realtime message) {
        String symbolName = symbolRepository.getSymbolNameByBrokerSymbol(message.getS());
        return RealTimeDTO.parse(message, symbolName);
    }

    @Override
    @Deprecated
    protected RealTimeDTO buildSnapshot(RealTimeDTO realTimeDTO) {
        this.snapshotData = filterByOpenTime(new LinkedList<>(realTimeMap.values()));
        return realTimeDTO;
    }

    @Override
    protected void metric(Realtime dto) {
        PushMetrics.pushedMessageLatency(dto.getExchangeId(), broker.name(),
            System.currentTimeMillis() - Long.parseLong(dto.getT()));
    }

    private void buildSnapshot() {
        this.snapshotData = filterByOpenTime(new LinkedList<>(realTimeMap.values()));
    }

    @Override
    public void clearSnapshot() {
        this.snapshotData = new LinkedList<>();
    }

    @Override
    public void onEngineMessage(Realtime data) {
        if (this.symbolType != ALL
            && this.symbolRepository.getSymbolType(data.getS()) != this.symbolType) {
            return;
        }
        String symbolName = symbolRepository.getSymbolNameByBrokerSymbol(data.getS());
        RealTimeDTO realTimeDTO = RealTimeDTO.parse(data, symbolName);
        QuoteIndex quoteIndex = QuoteIndex.builder()
            .exchangeId(data.getExchangeId())
            .symbol(data.getS())
            .build();
        realTimeMap.put(quoteIndex, realTimeDTO);
        synchronized (pendingMap) {
            pendingMap.put(quoteIndex, realTimeDTO);
        }
        buildSnapshot();
    }

    public void removeSymbols(Collection<Symbol> symbolSet) {
        for (Symbol symbol : symbolSet) {
            QuoteIndex quoteIndex = QuoteIndex.builder()
                .exchangeId(symbol.getExchangeId())
                .symbol(symbol.getSymbolId())
                .build();
            realTimeMap.remove(quoteIndex);
            log.info("Removed [{}:{}] from broker list [{}] type [{}]",
                symbol.getExchangeId(), symbol.getSymbolId(), orgId, this.symbolType);
        }
    }

    private void sendData() {
        try {
            if (!pendingMap.isEmpty()) {
                List<RealTimeDTO> changedTicker;
                synchronized (pendingMap) {
                    changedTicker = getChangedTickerList();
                    pendingMap.clear();
                }
                if (CollectionUtils.isNotEmpty(changedTicker)) {
                    long sendTime = System.currentTimeMillis();
                    clientMessageMap.forEach((c, clientMessage) -> {
                        ClientMessage msg = ClientMessage.builder()
                            .cid(clientMessage.getCid())
                            .symbol(symbol)
                            .topic(topic.name())
                            .data(changedTicker)
                            .params(clientMessage.getParams())
                            .f(false)
                            .sendTime(sendTime)
                            .build();
                        c.writeAndFlush(msg);
                    });
                }
            }
        } catch (Exception e) {
            log.error("Send msg ex: ", e);
        }
    }

    private List<RealTimeDTO> getChangedTickerList() {
        List<RealTimeDTO> realTimeDTOList = new ArrayList<>(pendingMap.values());
        return filterByOpenTime(realTimeDTOList);
    }

    public LinkedList<RealTimeDTO> filterByOpenTime(List<RealTimeDTO> realTimeDTOList) {
        LinkedList<RealTimeDTO> resultList = new LinkedList<>();
        for (RealTimeDTO realTimeDTO : realTimeDTOList) {
            if (symbolRepository.isOpenSymbol(this.orgId, realTimeDTO.getSymbol())) {
                resultList.add(realTimeDTO);
            } else if (log.isDebugEnabled()) {
                log.debug("No OpenTime Symbol!{},{}", this.orgId, realTimeDTO.getSymbol());
            }
        }
        return resultList;
    }

    @Override
    public void watching() {
        scheduleSendTask();
    }

    private void scheduleSendTask() {
        try {
            sendData();
        } finally {
            this.scheduledTaskExecutor.schedule(this::scheduleSendTask, 500, TimeUnit.MILLISECONDS);
        }
    }

    public List<RealTimeDTO> getCurrentTickers() {
        List<RealTimeDTO> realTimeDTOList = new ArrayList<>(this.realTimeMap.values());
        realTimeDTOList = filterByOpenTime(realTimeDTOList);
        return realTimeDTOList;
    }

}
