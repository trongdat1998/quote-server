package io.bhex.broker.quote.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ClientMessageV2 {

    private String topic;
    private String event;
    private Map<String, String> params;

    /**
     * response时带的data
     */
    private Object data;

    private String code;
    private String msg;

}
