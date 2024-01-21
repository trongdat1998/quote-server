package io.bhex.broker.quote.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ReconnectionInfo {
    private String host;
    private int port;
    private String partitionName;
}
