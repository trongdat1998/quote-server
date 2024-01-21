package io.bhex.broker.quote.handler;

import io.bhex.broker.quote.data.ClientMessageV2;
import io.bhex.broker.quote.enums.ErrorCodeEnum;
import io.bhex.broker.quote.enums.EventEnum;
import io.bhex.broker.quote.enums.TopicEnum;
import io.bhex.broker.quote.exception.BizException;
import io.bhex.broker.quote.listener.v2.TicketListener;
import io.bhex.broker.quote.repository.SymbolRepository;
import io.bhex.broker.quote.util.BeanUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;

import static io.bhex.broker.quote.common.Constants.SYMBOL;
import static io.bhex.broker.quote.common.Constants.SYMBOL_NAME;
import static org.apache.http.HttpHeaders.HOST;

@Component
public class TicketHandler implements ITopicHandler {
    private final SymbolRepository symbolRepository;
    private final ApplicationContext context;

    @Autowired
    public TicketHandler(SymbolRepository symbolRepository, ApplicationContext context) {
        this.symbolRepository = symbolRepository;
        this.context = context;
    }

    @Override
    public void handle(ChannelHandlerContext ctx, ClientMessageV2 msg) {
        Map<String, String> params = msg.getParams();
        if (Objects.isNull(params)) {
            ctx.channel().writeAndFlush(ErrorCodeEnum.SYMBOL_REQUIRED);
            return;
        }

        String symbol = params.get(SYMBOL);
        if (StringUtils.isEmpty(symbol)) {
            ctx.channel().writeAndFlush(ErrorCodeEnum.SYMBOL_REQUIRED);
            return;
        }

        Map<String, String> filteredParams = HandlerUtils.getFilterCommonParams(msg);
        filteredParams.put(SYMBOL, symbol);
        msg.setParams(filteredParams);

        try {
            Long orgId = symbolRepository.getOrgIdByDomain(ctx.channel()
                .attr(AttributeKey.<String>valueOf(HOST)).get());
            String symbolId = symbolRepository.getRealSymbolId(orgId, symbol);
            String symbolName = symbolRepository.getSymbolNameByBrokerSymbol(symbolId);
            filteredParams.put(SYMBOL, symbolId);
            filteredParams.put(SYMBOL_NAME, symbolName);
            msg.setParams(filteredParams);
            Long exchangeId = symbolRepository.getExchangeIdBySymbolAndOrgId(symbolId, orgId);
            Long sharedExchangeId = symbolRepository.getSharedExchangeId(exchangeId, symbolId);
            TicketListener ticketListener = context.getBean(BeanUtils.getTradeV2BeanName(sharedExchangeId, symbolId),
                TicketListener.class);
            EventEnum event = EventEnum.valueOF(msg.getEvent());
            if (EventEnum.SUB.equals(event)) {
                ticketListener.subscribe(ctx.channel(), msg);
            } else if (EventEnum.CANCEL.equals(event)) {
                ticketListener.unsubscribe(ctx.channel(), msg);
            }
        } catch (BizException e) {
            ctx.channel().writeAndFlush(e.getErrorCodeEnum());
        }
    }

    @Override
    public boolean isSameTopic(ClientMessageV2 msg) {
        return StringUtils.equalsIgnoreCase(TopicEnum.trade.name(), msg.getTopic());
    }
}
