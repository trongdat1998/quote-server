package io.bhex.broker.quote.listener;

import io.bhex.base.match.Ticket;
import io.bhex.broker.quote.data.ClientMessage;
import io.bhex.broker.quote.data.dto.TicketDTO;
import io.bhex.broker.quote.enums.TopicEnum;
import io.bhex.broker.quote.metrics.PushMetrics;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.context.ApplicationContext;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

@Slf4j
public class TradeListener extends AbstractDataListener<Ticket, TicketDTO, ConcurrentLinkedDeque<TicketDTO>> {
    public TradeListener(String symbol, TopicEnum topic, Class dataTypeClass, ApplicationContext context) {
        super(symbol, topic, dataTypeClass, new ConcurrentLinkedDeque<>(), context);
    }

    public static final int TRADE_CACHED_SIZE = 60;

    @Override
    protected ConcurrentLinkedDeque<TicketDTO> handleSnapshot(ClientMessage clientMessage) {
        return snapshotData;
    }

    @Override
    protected TicketDTO onMessage(Ticket message) {
        if (log.isDebugEnabled()) {
            log.debug("Received ticket [{}:{}:{}:{}:{}]",
                message.getExchangeId(), message.getSymbolId(),
                message.getPrice().getStr(), message.getQuantity().getStr(),
                message.getTicketId());
        }
        PushMetrics.receivedMessageFromGRpc(message.getExchangeId(), this.topic.name(),
            System.currentTimeMillis() - message.getTime());
        return TicketDTO.parse(message);
    }

    @Override
    protected TicketDTO buildSnapshot(TicketDTO ticketDTO) {
        snapshotData.addLast(ticketDTO);
        while (snapshotData.size() > TRADE_CACHED_SIZE) {
            snapshotData.removeFirst();
        }
        return ticketDTO;
    }

    @Override
    protected void metric(Ticket dto) {
        PushMetrics.pushedMessageLatency(dto.getExchangeId(), topic.name(), System.currentTimeMillis() - dto.getTime());
    }

    @Override
    public void clearSnapshot() {
        this.snapshotData.clear();
    }

    public void reloadHistoryData(List<Ticket> ticketList) {
        if (CollectionUtils.isNotEmpty(ticketList)) {
            this.snapshotData = getReverseTicketDTOList(ticketList);
            this.lastTime = this.snapshotData.getLast().getCurId();
        }
    }

    private ConcurrentLinkedDeque<TicketDTO> getReverseTicketDTOList(List<Ticket> ticketList) {
        ConcurrentLinkedDeque<TicketDTO> ticketDTOConcurrentLinkedDeque = new ConcurrentLinkedDeque<>();
        for (int i = ticketList.size() - 1; i >= 0; i--) {
            ticketDTOConcurrentLinkedDeque.addLast(TicketDTO.parse(ticketList.get(i)));
        }

        return ticketDTOConcurrentLinkedDeque;
    }
}
