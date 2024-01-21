package io.bhex.broker.quote.client;

import io.bhex.base.quote.QuoteRequest;
import io.bhex.ex.quote.core.enums.CommandType;
import io.grpc.stub.StreamObserver;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.concurrent.atomic.AtomicInteger;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class QuoteStreamInfo {
    private Long exchangeId;
    private String symbol;
    private CommandType commandType;
    private QuoteStreamObserver localStream;
    private StreamObserver<QuoteRequest> remoteStream;
    private AtomicInteger hbCnt;
}
