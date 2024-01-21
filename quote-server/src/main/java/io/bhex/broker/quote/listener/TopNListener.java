package io.bhex.broker.quote.listener;

import io.bhex.base.quote.Realtime;
import io.bhex.broker.grpc.basic.Symbol;
import io.bhex.broker.quote.data.ClientMessage;
import io.bhex.broker.quote.data.QuoteIndex;
import io.bhex.broker.quote.data.dto.RealTimeDTO;
import io.bhex.broker.quote.enums.TopicEnum;
import io.bhex.broker.quote.metrics.PushMetrics;
import io.bhex.broker.quote.repository.SymbolRepository;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.context.ApplicationContext;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.bhex.broker.quote.enums.StringsConstants.DEFAULT_TOP_N;
import static io.bhex.broker.quote.enums.StringsConstants.LIMIT;
import static io.bhex.broker.quote.enums.TopicEnum.topN;

@Slf4j
public class TopNListener extends AbstractDataListener<Realtime, RealTimeDTO, List<RealTimeDTO>> {
    private Long orgId;

    /**
     * 私有云 orgId下、symbolId唯一
     */
    private ConcurrentMap<String, RealTimeDTO> realTimeMap = new ConcurrentHashMap<>();
    // decrement
    private final Object CHANGED_LOCK = new Object();
    @GuardedBy("CHANGED_LOCK")
    private volatile boolean changed = false;
    // -1 表示所有币对
    private final int symbolType;
    public static final int ALL = -1;

    private static final Comparator<RealTimeDTO> MARGIN_COMPARATOR = (a, b) -> -a.getMargin().compareTo(b.getMargin());
    private SymbolRepository symbolRepository;

    public TopNListener(Long orgId, TopicEnum topic, Class dataTypeClass,
                        ScheduledExecutorService scheduledExecutorService,
                        int symbolType,
                        ApplicationContext context) {
        super(StringUtils.EMPTY, topic, dataTypeClass, new LinkedList<>(), context);
        this.orgId = orgId;
        this.symbolType = symbolType;
        this.scheduledTaskExecutor = scheduledExecutorService;
        this.symbolRepository = context.getBean(SymbolRepository.class);
    }

    @Override
    protected List<RealTimeDTO> handleSnapshot(ClientMessage clientMessage) {
        int n = getTopLimit(clientMessage);
        if (n > this.snapshotData.size()) {
            return this.snapshotData;
        }
        return getTopN(this.snapshotData, n);
    }

    @Override
    protected RealTimeDTO onMessage(Realtime message) {
        String symbolName = symbolRepository.getSymbolNameByBrokerSymbol(message.getS());
        return RealTimeDTO.parse(message, symbolName);
    }

    @Override
    protected RealTimeDTO buildSnapshot(RealTimeDTO realTimeDTO) {
        return realTimeDTO;
    }

    @Override
    protected void metric(Realtime dto) {
        PushMetrics.pushedMessageLatency(dto.getExchangeId(), topN.name(),
            System.currentTimeMillis() - Long.parseLong(dto.getT()));
    }

    @Override
    public void clearSnapshot() {
        this.snapshotData = null;
    }

    @Override
    public void onEngineMessage(Realtime data) {
        String symbolName = symbolRepository.getSymbolNameByBrokerSymbol(data.getS());
        RealTimeDTO realTimeDTO = RealTimeDTO.parse(data, symbolName);
        String symbol = data.getS();
        if (this.symbolType == ALL
            || symbolRepository.getSymbolType(symbol) == this.symbolType) {
            synchronized (CHANGED_LOCK) {
                realTimeMap.put(realTimeDTO.getSymbol(), realTimeDTO);
                if (!this.changed) {
                    this.changed = true;
                }
            }
        }
    }

    /**
     * 每1s发送一次
     */
    @Override
    public void watching() {
        this.future = scheduledTaskExecutor.schedule(() -> {
            try {
                scheduleTask();
            } catch (Exception e) {
                log.error("Send msg ex: ", e);
            }
        }, 1, TimeUnit.SECONDS);
    }

    private void scheduleTask() {
        try {
            consumeData();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            future = scheduledTaskExecutor.schedule(this::scheduleTask, 1, TimeUnit.SECONDS);
        }
    }

    private void consumeData() {
        if (this.changed) {
            synchronized (CHANGED_LOCK) {
                this.snapshotData = new ArrayList<>(realTimeMap.values());
                this.snapshotData.sort(MARGIN_COMPARATOR);
                this.snapshotData = filterTopN(this.snapshotData);
                this.changed = false;
            }
            if (CollectionUtils.isNotEmpty(this.snapshotData)) {
                for (Map.Entry<Channel, ClientMessage> channelClientMessageEntry : clientMessageMap.entrySet()) {
                    Channel c = channelClientMessageEntry.getKey();
                    ClientMessage clientMessage = channelClientMessageEntry.getValue();
                    int n = getTopLimit(clientMessage);
                    ClientMessage msg = ClientMessage.builder()
                        .cid(clientMessage.getCid())
                        .symbol(symbol)
                        .topic(topic.name())
                        .data(getTopN(this.snapshotData, n))
                        .params(clientMessage.getParams())
                        .f(false)
                        .build();
                    c.writeAndFlush(msg);
                }
            }
        }
    }

    public List<RealTimeDTO> filterTopN(List<RealTimeDTO> realTimeDTOList) {
        List<RealTimeDTO> resultList = new ArrayList<>();
        for (RealTimeDTO realTimeDTO : realTimeDTOList) {
            if (symbolRepository.isTopNOpenSymbol(this.orgId, realTimeDTO.getSymbol())) {
                resultList.add(realTimeDTO);
            }
        }
        return resultList;
    }

    /**
     * 删除存在于当前列表中的币对
     */
    public void removeSymbols(Collection<Symbol> symbolSet) {
        synchronized (CHANGED_LOCK) {
            for (Symbol symbol : symbolSet) {
                QuoteIndex quoteIndex = QuoteIndex.builder()
                    .exchangeId(symbol.getExchangeId())
                    .symbol(symbol.getSymbolId())
                    .build();
                realTimeMap.remove(quoteIndex);
                log.info("Removed [{}:{}] from broker topN [{}] type [{}]",
                    symbol.getExchangeId(), symbol.getSymbolId(), this.orgId, this.symbolType);
            }
            this.changed = true;
        }
    }

    /**
     * 同一个base token的，只显示一个
     * diff 表示可以删除的行情个数
     * 1. 倒序遍历
     * [btcusdt 0] -> [ltcusdt 1] -> [ethusdt 2]
     * < ----     < ----
     * <p>
     * 2. 把行情不断加入另一个链表的头上
     * <p>
     * 3. 如果发现有重复的baseToken币对时，把当前列表中之前的行情删除，把当前行情加到这个列表头，并diff - 1
     * <p>
     * 4. 当不能删除时break
     * <p>
     * 5. 最终得到结果有
     * 5.1 有重复币对，但是够n个的列表
     * 5.2 无重复币对，比n个还要多
     */
    private List<RealTimeDTO> getTopN(List<RealTimeDTO> realTimeDTOList, int n) {
        if (realTimeDTOList.size() <= n) {
            return realTimeDTOList;
        }
        // 可删除的个数
        int diff = realTimeDTOList.size() - n;
        LinkedList<RealTimeDTO> resultList = new LinkedList<>();
        Map<String, RealTimeDTO> realTimeDTOMap = new HashMap<>();
        for (int i = realTimeDTOList.size() - 1; i >= 0; i--) {
            RealTimeDTO realTimeDTO = realTimeDTOList.get(i);
            Symbol symbol = symbolRepository.getSymbol(realTimeDTO.getSymbol());
            if (Objects.isNull(symbol)) {
                log.warn("There is no [{}] symbolObj", realTimeDTO.getSymbol());
                resultList.addFirst(realTimeDTO);
                realTimeDTOMap.put(realTimeDTO.getSymbol(), realTimeDTO);
                continue;
            }

            // 删除已经重复的
            if (realTimeDTOMap.containsKey(symbol.getBaseTokenId())) {
                RealTimeDTO pendingRemovedRealTime = realTimeDTOMap.get(symbol.getBaseTokenId());
                resultList.remove(pendingRemovedRealTime);
                diff--;
            }

            realTimeDTOMap.put(symbol.getBaseTokenId(), realTimeDTO);
            resultList.addFirst(realTimeDTO);

            // 无法删除重复的quoteToken币对行情
            if (diff <= 0) {
                while (--i >= 0) {
                    resultList.addFirst(realTimeDTOList.get(i));
                }
                break;
            }

        }
        if (resultList.size() <= n) {
            return resultList;
        }
        return resultList.subList(0, n);
    }

    public List<RealTimeDTO> getTopN(int n) {
        List<RealTimeDTO> realTimeDTOS = new ArrayList<>(this.realTimeMap.values());
        realTimeDTOS.sort(MARGIN_COMPARATOR);
        realTimeDTOS = filterTopN(realTimeDTOS);
        return getTopN(realTimeDTOS, n);
    }

    private int getTopLimit(ClientMessage clientMessage) {
        Map<String, String> params = clientMessage.getParams();
        String n = params.get(LIMIT);
        return NumberUtils.toInt(n, DEFAULT_TOP_N);
    }

}
