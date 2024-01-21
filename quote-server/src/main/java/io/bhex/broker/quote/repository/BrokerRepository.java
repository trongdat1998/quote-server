package io.bhex.broker.quote.repository;

import io.bhex.broker.grpc.basic.BasicServiceGrpc;
import io.bhex.broker.grpc.basic.QuerySymbolRequest;
import io.bhex.broker.grpc.basic.QuerySymbolResponse;
import io.bhex.broker.grpc.basic.Symbol;
import io.bhex.broker.grpc.basic.TokenOptionInfo;
import io.bhex.broker.grpc.broker.Broker;
import io.bhex.broker.grpc.broker.BrokerServiceGrpc;
import io.bhex.broker.grpc.broker.QueryBrokerRequest;
import io.bhex.broker.grpc.broker.QueryBrokerResponse;
import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.gateway.GetRealQuoteEngineAddressRequest;
import io.bhex.broker.grpc.gateway.GetRealQuoteEngineAddressResponse;
import io.bhex.broker.grpc.gateway.QuoteEngineAddress;
import io.bhex.broker.grpc.gateway.QuoteServiceGrpc;
import io.bhex.broker.quote.config.GrpcRunner;
import io.grpc.ManagedChannel;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.bhex.base.token.TokenCategory.OPTION_CATEGORY;

@Component
public class BrokerRepository {

    @Getter
    private final SymbolRepository symbolRepository;
    private final GrpcRunner grpcRunner;

    private final long CONNECT_TIMEOUT = 5000L;

    @Autowired
    public BrokerRepository(SymbolRepository symbolRepository,
                            GrpcRunner grpcRunner) {
        this.symbolRepository = symbolRepository;
        this.grpcRunner = grpcRunner;
    }

    public List<Symbol> getSymbols() {
        ManagedChannel channel = grpcRunner.borrowBrokerChannel();
        BasicServiceGrpc.BasicServiceBlockingStub basicServiceBlockingStub = BasicServiceGrpc.newBlockingStub(channel)
            .withDeadlineAfter(CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
        QuerySymbolResponse response = basicServiceBlockingStub.querySymbols(QuerySymbolRequest.getDefaultInstance());
        if (response.getRet() != 0) {
            throw new IllegalStateException();
        }
        List<Symbol> symbolList = response.getSymbolList();
        List<Symbol> onlineSymbolList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(symbolList)) {
            for (Symbol symbol : symbolList) {
                if (symbol.getCategory() == OPTION_CATEGORY.getNumber()) {
                    // 期权
                    long currentTime = System.currentTimeMillis();
                    TokenOptionInfo tokenOptionInfo = symbol.getTokenOption();
                    if (currentTime < tokenOptionInfo.getSettlementDate()
                        && currentTime >= tokenOptionInfo.getIssueDate()) {
                        onlineSymbolList.add(symbol);
                    }
                } else {
                    onlineSymbolList.add(symbol);
                }
            }
        }
        return onlineSymbolList;
    }

    public List<Broker> getBrokers() {
        ManagedChannel channel = grpcRunner.borrowBrokerChannel();
        BrokerServiceGrpc.BrokerServiceBlockingStub brokerServiceBlockingStub =
            BrokerServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
        QueryBrokerResponse response = brokerServiceBlockingStub.queryBrokers(QueryBrokerRequest.getDefaultInstance());
        return response.getBrokersList();
    }

    public GetRealQuoteEngineAddressResponse getQuoteEngineAddress(String hostName, int port, String platform) {
        ManagedChannel channel = grpcRunner.borrowBrokerChannel();
        QuoteServiceGrpc.QuoteServiceBlockingStub quoteServiceBlockingStub =
            QuoteServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
        return quoteServiceBlockingStub.getRealQuoteEngineAddress(GetRealQuoteEngineAddressRequest.newBuilder()
            .setAddress(QuoteEngineAddress.newBuilder()
                .setHost(hostName)
                .setPort(port)
                .build())
            .setPlatform(platform)
            .build());
    }

    public List<Symbol> getSymbolListByOrgId(Long orgId) {
        ManagedChannel channel = grpcRunner.borrowBrokerChannel();
        BasicServiceGrpc.BasicServiceBlockingStub basicServiceBlockingStub = BasicServiceGrpc.newBlockingStub(channel)
            .withDeadlineAfter(CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
        QuerySymbolResponse response = basicServiceBlockingStub
            .querySymbols(QuerySymbolRequest.newBuilder()
                .setHeader(Header.newBuilder()
                    .setOrgId(orgId)
                    .build())
                .build());
        List<Symbol> symbolList = response.getSymbolList();
        List<Symbol> onlineSymbolList = new ArrayList<>();

        for (Symbol symbol : symbolList) {
            if (symbol.getCategory() == OPTION_CATEGORY.getNumber()) {
                // 期权
                long currentTime = System.currentTimeMillis();
                TokenOptionInfo tokenOptionInfo = symbol.getTokenOption();
                if (currentTime < tokenOptionInfo.getSettlementDate()
                    && currentTime >= tokenOptionInfo.getIssueDate()) {
                    onlineSymbolList.add(symbol);
                }
            } else {
                onlineSymbolList.add(symbol);
            }
        }
        return onlineSymbolList;
    }
}
