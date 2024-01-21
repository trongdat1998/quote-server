package io.bhex.broker.quote.listener;

import io.bhex.base.quote.MarkPrice;
import io.bhex.broker.quote.data.ClientMessage;
import io.bhex.broker.quote.data.dto.MarkPriceDTO;
import io.bhex.broker.quote.enums.TopicEnum;
import org.springframework.context.ApplicationContext;

public class MarkPriceListener extends AbstractExchangeSymbolDataListenerV2<MarkPrice, MarkPriceDTO, MarkPriceDTO> {
    public MarkPriceListener(Long exchangeId, String symbolId, ApplicationContext context) {
        super(exchangeId, symbolId, TopicEnum.markPrice, context);
    }

    @Override
    protected MarkPriceDTO handleSnapshot(ClientMessage clientMessage) {
        return super.snapshot;
    }

    @Override
    protected MarkPriceDTO convertFrom(MarkPrice data) {
        return MarkPriceDTO.parseFrom(data);
    }

    @Override
    protected MarkPriceDTO buildSnapshot(MarkPriceDTO markPriceDTO) {
        return markPriceDTO;
    }
}
