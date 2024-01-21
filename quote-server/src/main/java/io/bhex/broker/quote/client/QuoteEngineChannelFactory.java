package io.bhex.broker.quote.client;

import io.bhex.exchange.lang.AddressConfig;
import io.grpc.Channel;
import io.grpc.netty.NettyChannelBuilder;

public class QuoteEngineChannelFactory implements IGRpcChannelFactory {

    private final AddressConfig quoteEngineAddress;

    public QuoteEngineChannelFactory(AddressConfig quoteEngineAddress) {
        this.quoteEngineAddress = quoteEngineAddress;
    }

    @Override
    public Channel createChannel() {
        NettyChannelBuilder builder = NettyChannelBuilder
            .forAddress(this.quoteEngineAddress.getHost(), this.quoteEngineAddress.getPort())
            .usePlaintext();
        return builder.build();
    }
}
