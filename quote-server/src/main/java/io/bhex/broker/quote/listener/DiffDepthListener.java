package io.bhex.broker.quote.listener;

import io.bhex.base.quote.Depth;
import io.bhex.broker.grpc.basic.Symbol;
import io.bhex.broker.quote.data.ClientMessage;
import io.bhex.broker.quote.data.dto.DepthDTO;
import io.bhex.broker.quote.enums.TopicEnum;
import io.bhex.broker.quote.metrics.PushMetrics;
import io.bhex.broker.quote.repository.SymbolRepository;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.ApplicationContext;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.bhex.broker.quote.enums.StringsConstants.ORG_ID;

@Slf4j
public class DiffDepthListener extends AbstractDataListener<Depth, DepthDTO, DepthDTO> {
    private SymbolRepository symbolRepository;

    public DiffDepthListener(String symbol, TopicEnum topic, Class dataTypeClass, ApplicationContext context) {
        super(symbol, topic, dataTypeClass, context);
        symbolRepository = context.getBean(SymbolRepository.class);
    }

    private Depth lastDepth;
    private String lastVersion;
    private AtomicLong currentOrder = new AtomicLong(0);
    private static final long LOOP_SIZE = Integer.MAX_VALUE;

    @Override
    protected DepthDTO handleSnapshot(ClientMessage clientMessage) {
        return snapshotData;
    }

    @Override
    protected DepthDTO onMessage(Depth message) {
        PushMetrics.receivedMessageFromGRpc(message.getExchangeId(), this.topic.name(), System.currentTimeMillis() - message.getTime());
        String symbolName = symbolRepository.getSymbolNameByBrokerSymbol(message.getSymbol());
        return DepthDTO.parse(message, symbolName);
    }

    @Override
    protected DepthDTO buildSnapshot(DepthDTO depthDTO) {
        DepthDTO diffDepth = depthDTO;
        if (Objects.isNull(snapshotData)) {
            snapshotData = depthDTO;
        } else {
            diffDepth = diffOrderBook(snapshotData, depthDTO);
            snapshotData = depthDTO;
        }
        return diffDepth;
    }

    @Override
    protected void metric(Depth dto) {
        PushMetrics.pushedMessageLatency(dto.getExchangeId(), topic.name(), System.currentTimeMillis() - dto.getTime());
    }

    @Override
    public void clearSnapshot() {
        snapshotData = null;
    }

    private DepthDTO diffOrderBook(DepthDTO older, DepthDTO newer) {
        List<List<BigDecimal>> oldBids = convertToDecimal(older.getBids());
        List<List<BigDecimal>> oldAsks = convertToDecimal(older.getAsks());

        List<List<BigDecimal>> newBids = convertToDecimal(newer.getBids());
        List<List<BigDecimal>> newAsks = convertToDecimal(newer.getAsks());

        List<List<BigDecimal>> incBids = diffBids(oldBids, newBids);
        List<List<BigDecimal>> incAsks = diffAsks(oldAsks, newAsks);

        return DepthDTO
            .builder()
            .asks(convertToString(incAsks))
            .bids(convertToString(incBids))
            .version(newer.getVersion())
            .time(newer.getTime())
            .build();
    }

    private List<String[]> convertToString(List<List<BigDecimal>> decimalDepthList) {
        List<String[]> strDepthList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(decimalDepthList)) {
            for (List<BigDecimal> decimalList : decimalDepthList) {
                String[] strings = new String[decimalList.size()];
                for (int i = 0; i < decimalList.size(); i++) {
                    strings[i] = decimalList.get(i).toPlainString();
                }
                strDepthList.add(strings);
            }
        }
        return strDepthList;
    }

    private List<List<BigDecimal>> convertToDecimal(List<String[]> strDepthList) {
        List<List<BigDecimal>> decimalDepthList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(strDepthList)) {
            for (String[] strings : strDepthList) {
                if (Objects.nonNull(strings)) {
                    List<BigDecimal> decimalList = new ArrayList<>();
                    for (String string : strings) {
                        if (Objects.nonNull(string)) {
                            decimalList.add(new BigDecimal(string));
                        }
                    }
                    decimalDepthList.add(decimalList);
                }
            }
        }
        return decimalDepthList;
    }

    private List<List<BigDecimal>> diffBids(List<List<BigDecimal>> oldBids, List<List<BigDecimal>> newBids) {
        return diff(oldBids, newBids, false);
    }

    private List<List<BigDecimal>> diffAsks(List<List<BigDecimal>> oldAsks, List<List<BigDecimal>> newAsks) {
        return diff(oldAsks, newAsks, true);
    }

    /**
     * @param oldList 从小到大
     * @param newList 从小到大
     * @return 增量
     */
    private List<List<BigDecimal>> diff(List<List<BigDecimal>> oldList, List<List<BigDecimal>> newList, boolean asc) {
        List<BigDecimal> oldItem;
        List<BigDecimal> newItem;
        int oSt = 0;
        int nSt = 0;
        List<List<BigDecimal>> result = new ArrayList<>();
        while (oSt < oldList.size() && nSt < newList.size()) {
            oldItem = oldList.get(oSt);
            newItem = newList.get(nSt);
            BigDecimal oldPrice = oldItem.get(0);
            BigDecimal oldAmount = oldItem.get(1);
            BigDecimal newPrice = newItem.get(0);
            BigDecimal newAmount = newItem.get(1);
            List<BigDecimal> resultItem = new ArrayList<>();
            int comRes = newPrice.compareTo(oldPrice);
            if (!asc) {
                comRes = -comRes;
            }
            switch (comRes) {
                case -1:
                    resultItem.add(newPrice);
                    resultItem.add(newAmount);
                    result.add(resultItem);
                    ++nSt;
                    break;
                case 0:
                    if (newAmount.compareTo(oldAmount) != 0) {
                        resultItem.add(newPrice);
                        resultItem.add(newAmount);
                        result.add(resultItem);
                    }
                    ++nSt;
                    ++oSt;
                    break;
                case 1:
                    resultItem.add(oldPrice);
                    resultItem.add(BigDecimal.ZERO);
                    result.add(resultItem);
                    ++oSt;
                    break;
                default:
            }
        }
        // 旧的全删除
        while (oSt < oldList.size()) {
            List<BigDecimal> resultItem = oldList.get(oSt++);
            resultItem.set(1, BigDecimal.ZERO);
            result.add(resultItem);
        }
        // 新的全添加
        while (nSt < newList.size()) {
            result.add(newList.get(nSt++));
        }
        return result;
    }

    @Override
    public void watching() {
        this.future = scheduledTaskExecutor.schedule(this::scheduleSendTask, 300, TimeUnit.MILLISECONDS);
    }

    private void scheduleSendTask() {
        try {
            consumeData();
        } catch (Exception e) {
            log.error("Send msg ex: ", e);
        } finally {
            this.future = scheduledTaskExecutor.schedule(this::scheduleSendTask, 300, TimeUnit.MILLISECONDS);
        }
    }

    private void consumeData() {
        Depth lastOne = lastDepth;
        if (Objects.isNull(lastOne)) {
            return;
        }

        if (StringUtils.equals(lastOne.getVersion(), lastVersion)) {
            return;
        }
        lastVersion = lastOne.getVersion();

        DepthDTO dto = onMessage(lastOne);
        dto = buildSnapshot(dto);
        dto.setOrder(this.currentOrder.getAndIncrement());
        if (this.currentOrder.get() == LOOP_SIZE) {
            this.currentOrder.compareAndSet(this.currentOrder.get(), 0L);
        }

        sendData(dto);
        metric(lastOne);
    }

    private void sendData(DepthDTO depthDTO) {
        List<DepthDTO> data = Collections.singletonList(depthDTO);
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
    public void onEngineMessage(Depth data) {
        lastDepth = data;
    }
}
