package io.bhex.broker.quote.mq;

import com.google.protobuf.InvalidProtocolBufferException;
import io.bhex.base.quote.MarkPrice;
import io.bhex.broker.quote.listener.MarkPriceListener;
import io.bhex.broker.quote.repository.QuoteRepository;
import io.bhex.broker.quote.util.BeanUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class MQMarkPriceListener implements MessageListenerConcurrently {
    private final ApplicationContext applicationContext;
    private final QuoteRepository quoteRepository;

    public MQMarkPriceListener(ApplicationContext context, QuoteRepository quoteRepository) {
        this.applicationContext = context;
        this.quoteRepository = quoteRepository;
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        for (MessageExt msg : msgs) {
            try {
                MarkPrice markPrice = MarkPrice.parseFrom(msg.getBody());
                MarkPriceListener markPriceListener = this.applicationContext.getBean(BeanUtils
                        .getMarkPriceBeanName(markPrice.getExchangeId(), markPrice.getSymbolId()),
                    MarkPriceListener.class);
                markPriceListener.onEngineMessage(markPrice);
                quoteRepository.updateMarkPrice(markPrice);
            } catch (InvalidProtocolBufferException e) {
                log.error(e.getMessage(), e);
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}
