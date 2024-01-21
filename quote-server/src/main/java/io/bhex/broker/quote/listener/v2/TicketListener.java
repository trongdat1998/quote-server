package io.bhex.broker.quote.listener.v2;

import io.bhex.base.match.Ticket;
import io.bhex.broker.quote.data.dto.TicketDTO;
import io.bhex.broker.quote.enums.TopicEnum;
import org.springframework.context.ApplicationContext;

public class TicketListener extends AbstractExchangeSymbolTopicListener<Ticket, TicketDTO> {
    public TicketListener(Long exchangeId, String symbolId, ApplicationContext context) {
        super(exchangeId, symbolId, context);
    }

    @Override
    protected TicketDTO convertFrom(Ticket data) {
        return TicketDTO.parse(data);
    }

    @Override
    public TopicEnum getTopic() {
        return TopicEnum.trade;
    }
}
