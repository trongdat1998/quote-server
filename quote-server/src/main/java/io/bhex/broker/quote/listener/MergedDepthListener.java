package io.bhex.broker.quote.listener;

import io.bhex.broker.quote.enums.TopicEnum;
import lombok.EqualsAndHashCode;
import org.springframework.context.ApplicationContext;

@EqualsAndHashCode(callSuper = true)
public class MergedDepthListener extends DepthListener implements IDumpScaleDepth {
    private Integer dumpScale;

    public MergedDepthListener(String symbol, Integer dumpScale, TopicEnum topic, Class dataTypeClass,
                               ApplicationContext context) {
        super(symbol, topic, dataTypeClass, context);
        this.dumpScale = dumpScale;
    }

    @Override
    public Integer getDumpScale() {
        return this.dumpScale;
    }
}
