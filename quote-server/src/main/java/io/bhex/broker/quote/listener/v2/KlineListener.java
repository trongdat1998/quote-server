package io.bhex.broker.quote.listener.v2;

import io.bhex.base.quote.KLine;
import io.bhex.broker.quote.data.dto.KlineItemDTO;
import io.bhex.broker.quote.enums.TopicEnum;
import io.bhex.broker.quote.repository.SymbolRepository;
import org.springframework.context.ApplicationContext;

public class KlineListener extends AbstractExchangeSymbolTopicListener<KLine, KlineItemDTO> {
    private String interval;
    private SymbolRepository symbolRepository;

    public KlineListener(Long exchangeId, String symbolId, String interval, ApplicationContext context) {
        super(exchangeId, symbolId, context);
        this.interval = interval;
        this.symbolRepository = context.getBean(SymbolRepository.class);
    }

    @Override
    protected KlineItemDTO convertFrom(KLine data) {
        String symbolName = symbolRepository.getSymbolNameByBrokerSymbol(data.getSymbol());
        return KlineItemDTO.parse(data, symbolName);
    }

    @Override
    public TopicEnum getTopic() {
        return TopicEnum.kline;
    }
}
