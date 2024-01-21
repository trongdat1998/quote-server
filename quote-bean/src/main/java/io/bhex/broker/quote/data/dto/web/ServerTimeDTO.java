package io.bhex.broker.quote.data.dto.web;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ServerTimeDTO {
    /**
     * 服务器时间
     */
    @JsonProperty("time")
    private long serverTime;
}
