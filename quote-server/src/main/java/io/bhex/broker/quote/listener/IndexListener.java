package io.bhex.broker.quote.listener;

import io.bhex.base.quote.Index;
import io.bhex.broker.quote.data.ClientMessage;
import io.bhex.broker.quote.data.dto.IndexDTO;
import io.bhex.broker.quote.enums.TopicEnum;
import org.springframework.context.ApplicationContext;

public class IndexListener extends AbstractDataListener<Index, IndexDTO, IndexDTO> {

    public IndexListener(String symbol, TopicEnum topic, Class dataTypeClass, ApplicationContext context) {
        super(symbol, topic, dataTypeClass, context);
    }

    @Override
    protected IndexDTO handleSnapshot(ClientMessage clientMessage) {
        return snapshotData;
    }

    @Override
    protected IndexDTO onMessage(Index message) {
        return IndexDTO.parse(message);
    }

    @Override
    protected IndexDTO buildSnapshot(IndexDTO indexDTO) {
        snapshotData = indexDTO;
        return indexDTO;
    }

    @Override
    protected void metric(Index dto) {
    }

    @Override
    public void clearSnapshot() {
        snapshotData = null;
    }
}
