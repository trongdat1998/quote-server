package io.bhex.broker.quote.listener;

import io.bhex.base.quote.Realtime;
import io.bhex.broker.grpc.basic.Symbol;
import io.bhex.broker.quote.data.ClientMessage;
import io.bhex.broker.quote.data.dto.RealTimeDTO;
import io.bhex.broker.quote.enums.TopicEnum;
import io.bhex.broker.quote.metrics.PushMetrics;
import io.bhex.broker.quote.repository.SymbolRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;

@Slf4j
public class RealTimeListener extends AbstractDataListener<Realtime, RealTimeDTO, RealTimeDTO> {
    private SymbolRepository symbolRepository;

    public RealTimeListener(String symbol, TopicEnum topic, Class dataTypeClass, ApplicationContext context) {
        super(symbol, topic, dataTypeClass, context);
        symbolRepository = context.getBean(SymbolRepository.class);
    }

    @Override
    protected RealTimeDTO handleSnapshot(ClientMessage clientMessage) {
        return snapshotData;
    }

    @Override
    public RealTimeDTO onMessage(Realtime message) {
        String symbolName = symbolRepository.getSymbolNameByBrokerSymbol(message.getS());
        return RealTimeDTO.parse(message, symbolName);
    }

    @Override
    public RealTimeDTO buildSnapshot(RealTimeDTO realTimeDTO) {
        this.snapshotData = realTimeDTO;
        return realTimeDTO;
    }

    @Override
    protected void metric(Realtime dto) {
        PushMetrics.pushedMessageLatency(dto.getExchangeId(), topic.name(), System.currentTimeMillis() - Long.parseLong(dto.getT()));
    }

    @Override
    public void clearSnapshot() {
        snapshotData = null;
    }

    @Override
    public void onEngineMessage(Realtime data) {
        if (dataQueue.size() > 100) {
            dataQueue.clear();
        }
        dataQueue.add(data);
    }

}
