package io.bhex.broker.quote.config;

import io.bhex.base.account.FuturesServerGrpc;
import io.bhex.base.account.OptionServerGrpc;
import io.bhex.base.grpc.annotation.GrpcServerStarter;
import io.bhex.base.grpc.client.channel.IGrpcClientPool;
import io.bhex.base.quote.QuoteServiceGrpc;
import io.bhex.base.quote.ThirdQuoteServiceGrpc;
import io.bhex.base.token.SymbolServiceGrpc;
import io.bhex.broker.quote.util.BrokerAuthUtil;
import io.bhex.ex.quote.QuoteMarketServiceGrpc;
import io.bhex.exchange.lang.AddressConfig;
import io.grpc.ManagedChannel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.Order;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import static io.bhex.broker.quote.enums.OrderConstants.CONNECTED_SERVER_ORDER;

/**
 * @author wangsc
 * @description gateway
 * @date 2020-06-12 16:18
 */
@GrpcServerStarter
@Slf4j
@ConditionalOnProperty(name = "bhex.proxy", havingValue = "true")
public class ProxyGrpcRunner extends GrpcRunner {

    private static final String GATEWAY_BH_SERVER = "bhServer";

    private static final String GATEWAY_QUOTE_DATA_SERVER = "quoteDataServer";

    private static final String GATEWAY_QUOTE_FARMER_SERVER = "farmerServer";

    @Resource
    private ApplicationContext applicationContext;

    @Autowired
    public ProxyGrpcRunner(BrokerServerConfig brokerServerConfig, IGrpcClientPool grpcClientPool,
                           @Qualifier("gateway-server") AddressConfig gatewayServerConfig) {
        super(brokerServerConfig, grpcClientPool, gatewayServerConfig, gatewayServerConfig);
    }

    @PostConstruct
    @Order(CONNECTED_SERVER_ORDER)
    @Override
    public void init() {
        log.info("Init grpc client connection is started.");
        grpcClientPool.setShortcut(brokerServerConfig.getName(),
            brokerServerConfig.getHost(), brokerServerConfig.getPort());
        grpcClientPool.setShortcut(dataServiceServerConfig.getName(),
            dataServiceServerConfig.getHost(), dataServiceServerConfig.getPort());
        grpcClientPool.setShortcut(bhServerAddressConfig.getName(),
            bhServerAddressConfig.getHost(), bhServerAddressConfig.getPort());
        BrokerAuthUtil.init(applicationContext);
        log.info("Init grpc client connection is ended.");
    }

    @Override
    public SymbolServiceGrpc.SymbolServiceBlockingStub symbolServiceBlockingStub(Long orgId) {
        ManagedChannel channel = borrowBhServerChannel();
        return SymbolServiceGrpc.newBlockingStub(channel)
            .withCallCredentials(new RouteAuthCredentials(GATEWAY_BH_SERVER, orgId));
    }

    @Override
    public OptionServerGrpc.OptionServerBlockingStub optionServerBlockingStub(Long orgId) {
        ManagedChannel channel = borrowBhServerChannel();
        return OptionServerGrpc.newBlockingStub(channel)
            .withCallCredentials(new RouteAuthCredentials(GATEWAY_BH_SERVER, orgId));
    }

    @Override
    public QuoteServiceGrpc.QuoteServiceBlockingStub quoteServiceBlockingStub(Long orgId) {
        ManagedChannel channel = borrowDataServiceChannel();
        return QuoteServiceGrpc.newBlockingStub(channel)
            .withCallCredentials(new RouteAuthCredentials(GATEWAY_QUOTE_DATA_SERVER, orgId));
    }

    @Override
    public ThirdQuoteServiceGrpc.ThirdQuoteServiceBlockingStub thirdQuoteServiceBlockingStub(Long orgId) {
        ManagedChannel channel = borrowDataServiceChannel();
        return ThirdQuoteServiceGrpc.newBlockingStub(channel)
            .withCallCredentials(new RouteAuthCredentials(GATEWAY_QUOTE_FARMER_SERVER, orgId));
    }

    @Override
    public QuoteMarketServiceGrpc.QuoteMarketServiceBlockingStub quoteMarketServiceBlockingStub(Long orgId) {
        ManagedChannel channel = borrowDataServiceChannel();
        return QuoteMarketServiceGrpc.newBlockingStub(channel)
            .withCallCredentials(new RouteAuthCredentials(GATEWAY_QUOTE_DATA_SERVER, orgId));
    }

    @Override
    public FuturesServerGrpc.FuturesServerBlockingStub futuresServerBlockingStub(Long orgId) {
        ManagedChannel channel = borrowBhServerChannel();
        return FuturesServerGrpc.newBlockingStub(channel)
            .withCallCredentials(new RouteAuthCredentials(GATEWAY_BH_SERVER, orgId));
    }
}
