package io.bhex.broker.quote.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ClientMessage {

    /**
     * ClientId 取消时，取消这个client中所有的订阅
     */
    @JsonProperty("id")
    private String cid;

    /**
     * exchangeId.symbol,exchangeId.symbol......
     */
    private String symbol;
    private String symbolName;

    private String topic;
    private String event;
    private Map<String, String> params;

    /**
     * response时带的data
     */
    private Object data;
    private boolean f;

    private String code;
    private String msg;

    @JsonIgnore
    private boolean isShared;
    @JsonIgnore
    private Long sharedExchangeId;

    private Long sendTime;

}
