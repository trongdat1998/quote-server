package io.bhex.broker.quote.listener;

import io.bhex.base.quote.Depth;
import io.bhex.broker.quote.data.ClientMessage;
import io.bhex.broker.quote.data.dto.DepthDTO;
import io.bhex.broker.quote.enums.TopicEnum;
import io.bhex.broker.quote.metrics.PushMetrics;
import io.bhex.broker.quote.repository.SymbolRepository;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.ApplicationContext;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static io.bhex.broker.quote.enums.StringsConstants.ORG_ID;

@Slf4j
public class DepthListener extends AbstractDataListener<Depth, DepthDTO, DepthDTO> {
    private SymbolRepository symbolRepository;

    public DepthListener(String symbol, TopicEnum topic, Class dataTypeClass, ApplicationContext context) {
        super(symbol, topic, dataTypeClass, context);
        this.symbolRepository = context.getBean(SymbolRepository.class);
    }

    private Depth lastDepth;
    private String lastVersion;

    @Override
    protected DepthDTO handleSnapshot(ClientMessage clientMessage) {
        return snapshotData;
    }

    @Override
    protected DepthDTO onMessage(Depth message) {
        String symbolName = symbolRepository.getSymbolNameByBrokerSymbol(message.getSymbol());
        return DepthDTO.parse(message, symbolName);
    }

    @Override
    protected DepthDTO buildSnapshot(DepthDTO depthDTO) {
        snapshotData = depthDTO;
        return depthDTO;
    }

    @Override
    protected void metric(Depth dto) {
        PushMetrics.pushedMessageLatency(dto.getExchangeId(), topic.name(), System.currentTimeMillis() - dto.getTime());
    }

    @Override
    public void clearSnapshot() {
        snapshotData = null;
    }

    @Override
    public void watching() {
        this.future = scheduledTaskExecutor.schedule(() -> {
            try {
                this.scheduleTask();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }, 1000, TimeUnit.MILLISECONDS);
    }

    private void scheduleTask() {
        try {
            consumeData();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            this.future = scheduledTaskExecutor.schedule(this::scheduleTask, 300, TimeUnit.MILLISECONDS);
        }
    }

    private void consumeData() {
        try {
            Depth lastOne = lastDepth;
            if (Objects.isNull(lastOne)) {
                return;
            }

            if (StringUtils.equals(lastOne.getVersion(), lastVersion)) {
                return;
            }

            this.lastVersion = lastOne.getVersion();
            DepthDTO dto = onMessage(lastOne);
            buildSnapshot(dto);
            sendData(dto);
            metric(lastOne);
        } catch (Exception e) {
            log.error("Send msg ex: ", e);
        }
    }

    private void sendData(DepthDTO depthDTO) {
        List<DepthDTO> data = Collections.singletonList(depthDTO);
        long sendTime = System.currentTimeMillis();
        clientMessageMap.forEach((channel, clientMessage) -> {
            Long orgId = (Long) channel.attr(AttributeKey.valueOf(ORG_ID)).get();
            String symbolId = symbolRepository.getRealSymbolId(orgId, symbol);
            String symbolName = symbolRepository.getSymbolName(orgId, symbolId);
            ClientMessage msg = ClientMessage.builder()
                .cid(clientMessage.getCid())
                .symbol(symbolId)
                .symbolName(symbolName)
                .topic(topic.name())
                .data(data)
                .params(clientMessage.getParams())
                .f(false)
                .sendTime(sendTime)
                .build();
            channel.writeAndFlush(msg);
        });
    }

    @Override
    public void onEngineMessage(Depth data) {
        this.lastDepth = data;
    }
}
