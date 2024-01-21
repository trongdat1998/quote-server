package io.bhex.broker.quote.data.dto.api;

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
    private long serverTime;
}
