package io.bhex.broker.quote.client;

import io.grpc.Channel;

public interface IGRpcChannelFactory {
    Channel createChannel();
}
