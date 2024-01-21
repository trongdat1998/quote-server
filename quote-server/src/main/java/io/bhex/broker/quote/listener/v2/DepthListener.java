package io.bhex.broker.quote.listener.v2;

import io.bhex.base.quote.Depth;
import io.bhex.broker.quote.data.dto.v2.DepthDTO;
import io.bhex.broker.quote.enums.TopicEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;

@Slf4j
public class DepthListener extends AbstractExchangeSymbolTopicListener<Depth, DepthDTO> {

    public DepthListener(Long exchangeId, String symbolId, ApplicationContext context) {
        super(exchangeId, symbolId, context);
    }

    @Override
    public TopicEnum getTopic() {
        return TopicEnum.depth;
    }

    @Override
    protected DepthDTO convertFrom(Depth data) {
        return DepthDTO.parse(data);
    }

}
