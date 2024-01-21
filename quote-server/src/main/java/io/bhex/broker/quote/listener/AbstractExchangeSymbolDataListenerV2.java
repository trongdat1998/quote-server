package io.bhex.broker.quote.listener;

import io.bhex.broker.quote.data.ClientMessage;
import io.bhex.broker.quote.data.dto.IEngineData;
import io.bhex.broker.quote.enums.TopicEnum;
import io.netty.channel.Channel;
import org.springframework.context.ApplicationContext;

import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Objects;

import static ch.qos.logback.core.CoreConstants.DOT;

public abstract class AbstractExchangeSymbolDataListenerV2<RECEIVED, DTO extends IEngineData, SNAPSHOT> extends AbstractDataListenerV2<RECEIVED, DTO, SNAPSHOT> {

    private Long exchangeId;
    private String symbolId;
    private String symbols;

    public AbstractExchangeSymbolDataListenerV2(Long exchangeId, String symbolId, TopicEnum topicEnum, ApplicationContext context) {
        super(topicEnum, context);
        this.exchangeId = exchangeId;
        this.symbolId = symbolId;
        this.symbols = exchangeId + DOT + symbolId;
    }

    @Override
    public void addSubscriber(Channel channel, ClientMessage clientMessage) {
        super.addSubscriber(channel, clientMessage);
        ClientMessage.ClientMessageBuilder builder = ClientMessage.builder();
        builder
            .cid(clientMessage.getCid())
            .symbol(symbolId)
            .topic(topicEnum.name())
            .f(true)
            .params(clientMessage.getParams());
        if (Objects.isNull(snapshot)) {
            builder.data(Collections.emptyList());
        } else {
            if (this.snapshot instanceof Deque
                || this.snapshot instanceof List) {
                builder.data(handleSnapshot(clientMessage));
            } else {
                builder.data(Collections.singletonList(handleSnapshot(clientMessage)));
            }
        }
        clientMessage.setSendTime(System.currentTimeMillis());
        channel.writeAndFlush(builder.build());
    }

    @Override
    public Long getExchangeId() {
        return exchangeId;
    }

    @Override
    public String getSymbol() {
        return this.symbolId;
    }

    @Override
    public String getSymbols() {
        return this.symbols;
    }
}
