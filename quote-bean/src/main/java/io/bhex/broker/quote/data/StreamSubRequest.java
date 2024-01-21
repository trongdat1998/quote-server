package io.bhex.broker.quote.data;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * 消息订阅事件对象
 */
@Data
@Builder
public class StreamSubRequest {
    private Long exchangeId;
    private String symbol;
    private String topic;
    private String klineType;
    private List content;
    private int subCmd;
}
