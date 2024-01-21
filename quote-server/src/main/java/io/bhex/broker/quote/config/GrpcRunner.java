package io.bhex.broker.quote.config;

import io.bhex.base.account.FuturesServerGrpc;
import io.bhex.base.account.OptionServerGrpc;
import io.bhex.base.grpc.annotation.GrpcServerStarter;
import io.bhex.base.grpc.client.channel.IGrpcClientPool;
import io.bhex.base.grpc.server.GrpcServerRunner;
import io.bhex.base.grpc.server.IGrpcServerRunnable;
import io.bhex.base.quote.QuoteServiceGrpc;
import io.bhex.base.quote.ThirdQuoteServiceGrpc;
import io.bhex.base.token.SymbolServiceGrpc;
import io.bhex.ex.quote.QuoteMarketServiceGrpc;
import io.bhex.exchange.lang.AddressConfig;
import io.grpc.ManagedChannel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.annotation.Order;

import javax.annotation.PostConstruct;

import static io.bhex.broker.quote.enums.OrderConstants.CONNECTED_SERVER_ORDER;

@GrpcServerStarter
@Slf4j
@ConditionalOnProperty(name = "bhex.proxy", havingValue = "false")
public class GrpcRunner implements IGrpcServerRunnable {

    final BrokerServerConfig brokerServerConfig;
    final IGrpcClientPool grpcClientPool;
    final AddressConfig dataServiceServerConfig;
    final AddressConfig bhServerAddressConfig;

    @Autowired
    public GrpcRunner(BrokerServerConfig brokerServerConfig,
                      IGrpcClientPool grpcClientPool,
                      @Qualifier("quote-data-service") AddressConfig quoteDataServiceConfig,
                      @Qualifier("bh-server") AddressConfig bhServerAddressConfig) {
        this.brokerServerConfig = brokerServerConfig;
        this.grpcClientPool = grpcClientPool;
        this.dataServiceServerConfig = quoteDataServiceConfig;
        this.bhServerAddressConfig = bhServerAddressConfig;
    }

    @PostConstruct
    @Order(CONNECTED_SERVER_ORDER)
    public void init() {
        log.info("Init grpc client connection is started.");
        grpcClientPool.setShortcut(brokerServerConfig.getName(),
            brokerServerConfig.getHost(), brokerServerConfig.getPort());
        grpcClientPool.setShortcut(dataServiceServerConfig.getName(),
            dataServiceServerConfig.getHost(), dataServiceServerConfig.getPort());
        grpcClientPool.setShortcut(bhServerAddressConfig.getName(),
            bhServerAddressConfig.getHost(), bhServerAddressConfig.getPort());
        log.info("Init grpc client connection is ended.");

        log.info("=====server settings==========");
        for (String shortcut : grpcClientPool.shortcuts()) {
            IGrpcClientPool.Address address = grpcClientPool.addressOf(shortcut);
            log.info("[{}:{}:{}]", shortcut, address.getHost(), address.getPort());
        }
    }

    @Override
    public void run(GrpcServerRunner grpcServerRunner) {
        try {
            grpcServerRunner.run();
        } catch (Exception e) {
            log.error("Grpc runner: ", e);
        }
    }

    public ManagedChannel borrowBrokerChannel() {
        return grpcClientPool.borrowChannel(brokerServerConfig.getName());
    }

    public ManagedChannel borrowDataServiceChannel() {
        return grpcClientPool.borrowChannel(dataServiceServerConfig.getName());
    }

    ManagedChannel borrowBhServerChannel() {
        return grpcClientPool.borrowChannel(bhServerAddressConfig.getName());
    }

    public SymbolServiceGrpc.SymbolServiceBlockingStub symbolServiceBlockingStub(Long orgId) {
        ManagedChannel channel = borrowBhServerChannel();
        return SymbolServiceGrpc.newBlockingStub(channel);
    }

    public OptionServerGrpc.OptionServerBlockingStub optionServerBlockingStub(Long orgId) {
        ManagedChannel channel = borrowBhServerChannel();
        return OptionServerGrpc.newBlockingStub(channel);
    }

    public QuoteServiceGrpc.QuoteServiceBlockingStub quoteServiceBlockingStub(Long orgId) {
        ManagedChannel channel = borrowDataServiceChannel();
        return QuoteServiceGrpc.newBlockingStub(channel);
    }

    public ThirdQuoteServiceGrpc.ThirdQuoteServiceBlockingStub thirdQuoteServiceBlockingStub(Long orgId) {
        ManagedChannel channel = borrowDataServiceChannel();
        return ThirdQuoteServiceGrpc.newBlockingStub(channel);
    }

    public QuoteMarketServiceGrpc.QuoteMarketServiceBlockingStub quoteMarketServiceBlockingStub(Long orgId) {
        ManagedChannel channel = borrowDataServiceChannel();
        return QuoteMarketServiceGrpc.newBlockingStub(channel);
    }

    public FuturesServerGrpc.FuturesServerBlockingStub futuresServerBlockingStub(Long orgId) {
        ManagedChannel channel = borrowBhServerChannel();
        return FuturesServerGrpc.newBlockingStub(channel);
    }
}
