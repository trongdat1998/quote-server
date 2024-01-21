package io.bhex.broker.quote.listener.v2;

import io.bhex.base.quote.Realtime;
import io.bhex.broker.quote.data.dto.v2.RealTimeDTO;
import io.bhex.broker.quote.enums.TopicEnum;
import org.springframework.context.ApplicationContext;

public class RealTimeListener extends AbstractExchangeSymbolTopicListener<Realtime, RealTimeDTO> {
    public RealTimeListener(Long exchangeId, String symbolId, ApplicationContext context) {
        super(exchangeId, symbolId, context);
    }

    @Override
    protected RealTimeDTO convertFrom(Realtime data) {
        return RealTimeDTO.parse(data);
    }

    @Override
    public TopicEnum getTopic() {
        return TopicEnum.realtimes;
    }
}
