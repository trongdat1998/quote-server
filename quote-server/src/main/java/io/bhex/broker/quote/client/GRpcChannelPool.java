package io.bhex.broker.quote.client;

import io.bhex.base.grpc.Constants;
import io.bhex.broker.quote.metrics.PushMetrics;
import io.grpc.Channel;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.netty.NettyChannelBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

@Slf4j
public class GRpcChannelPool {
    private final Object LOCK = new Object();
    private final int coreSize;
    private final IGRpcChannelFactory channelFactory;
    @GuardedBy("LOCK")
    private final Map<Channel, Integer> channels = new HashMap<>();
    @Getter
    private final String address;
    @Getter
    private final int port;
    private volatile ServerStatusEnum state;

    private static final int UNHEALTHY_CHANNEL_SIZE_CONTINUOUSLY = 2;
    public static final int DEFAULT_CHANNEL_POOL_SIZE = 5;
    private static final int MARK_DOWN_EFFECT_TIMES = 12;
    private int curMarkDownTime = 0;

    public GRpcChannelPool(int coreSize, String address, int port) {
        this.coreSize = coreSize;
        this.channelFactory = () -> NettyChannelBuilder.forAddress(address, port)
            .usePlaintext()
            .build();
        this.address = address;
        this.port = port;
        this.state = ServerStatusEnum.CONNECTED;
    }

    public GRpcChannelPool(String address, int port) {
        this(DEFAULT_CHANNEL_POOL_SIZE, address, port);
    }

    public void setConnected() {
        curMarkDownTime = 0;
        state = ServerStatusEnum.CONNECTED;
    }

    public void setDisconnected() {
        state = ServerStatusEnum.DISCONNECTED;
    }


    public boolean isConnected() {
        return ServerStatusEnum.CONNECTED.equals(state);
    }

    public void checkHealth() {
        List<Channel> unhealthyChannelList = new ArrayList<>();
        Set<Channel> channels = new CopyOnWriteArraySet<>(this.channels.keySet());
        int unhealthyChannelSize = 0;
        for (Channel channel : channels) {
            if (!isConnected()) {
                unhealthyChannelList.add(channel);
                continue;
            }

            HealthGrpc.HealthBlockingStub stub = HealthGrpc.newBlockingStub(channel)
                .withDeadline(Deadline.after(2, TimeUnit.SECONDS));
            try {
                HealthCheckResponse response = stub.check(HealthCheckRequest.newBuilder()
                    .setService(Constants.BHEX_GRPC_SERVICE)
                    .build());
                if (!HealthCheckResponse.ServingStatus.SERVING.equals(response.getStatus())) {
                    unhealthyChannelList.add(channel);
                    unhealthyChannelSize++;
                } else {
                    unhealthyChannelSize = 0;
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                unhealthyChannelList.add(channel);
                unhealthyChannelSize++;
            }

            if (unhealthyChannelSize > UNHEALTHY_CHANNEL_SIZE_CONTINUOUSLY) {
                setDisconnected();
            }
        }
        log.info("Server [{}] Channels total [{}] unhealthy [{}]",
            address, channels.size(), unhealthyChannelList.size());
        removeChannels(unhealthyChannelList);
    }

    public Channel pollChannel() {
        Channel channel = null;
        if (!isConnected()) {
            throw new IllegalStateException(String.format("Node [%s:%s] is down, cant get channel.",
                this.address, this.port));
        }
        synchronized (LOCK) {
            if (channels.size() < coreSize) {
                channel = channelFactory.createChannel();
                channels.put(channel, 1);
                PushMetrics.addQuoteEngineGRpcChannel();
                log.info("Allocate channel cur size [{}]", channels.size());
            } else {
                int min = Integer.MAX_VALUE;
                for (Map.Entry<Channel, Integer> managedChannelAtomicIntegerEntry : channels.entrySet()) {
                    Channel curChannel = managedChannelAtomicIntegerEntry.getKey();
                    Integer cnt = managedChannelAtomicIntegerEntry.getValue();
                    if (cnt < min) {
                        channel = curChannel;
                        min = cnt;
                    }
                }
                channels.put(channel, min + 1);
            }
        }
        return channel;
    }

    public void returnChannel(Channel channel) {
        synchronized (LOCK) {
            channels.computeIfPresent(channel, (ch, cnt) -> cnt--);
        }
    }

    public void markServerDown() {
        if (isConnected() && ++curMarkDownTime > MARK_DOWN_EFFECT_TIMES) {
            setDisconnected();
        }
    }

    public void removeChannel(Channel channel) {
        synchronized (LOCK) {
            channels.remove(channel);
            PushMetrics.removeQuoteEngineGRpcChannel();
            ((ManagedChannel) channel).shutdownNow();
            log.info("Remove channel cur size [{}]", channels.size());
        }
    }

    public void removeChannels(List<Channel> channels) {
        for (Channel channel : channels) {
            removeChannel(channel);
        }
    }

    public void shutdownChannel() {
        synchronized (LOCK) {
            channels.forEach((channel, integer) -> {
                ManagedChannel managedChannel = (ManagedChannel) channel;
                managedChannel.shutdownNow();
                try {
                    while (!managedChannel.isTerminated()) {
                        if (managedChannel.awaitTermination(5L, TimeUnit.SECONDS)) {
                            break;
                        }
                        Thread.sleep(5000L);
                    }
                } catch (Exception e) {
                    log.warn(e.getMessage(), e);
                }
            });
        }
    }

    @Slf4j
    private static class ShutdownCallBack implements Runnable {
        private Channel channel;
        private GRpcChannelPool pool;

        ShutdownCallBack(Channel channel, GRpcChannelPool pool) {
            this.channel = channel;
            this.pool = pool;
        }

        @Override
        public void run() {
            try {
                pool.removeChannel(channel);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public enum ServerStatusEnum {
        CONNECTED(1),
        DISCONNECTED(2);
        private int code;

        ServerStatusEnum(int code) {
            this.code = code;
        }
    }
}
