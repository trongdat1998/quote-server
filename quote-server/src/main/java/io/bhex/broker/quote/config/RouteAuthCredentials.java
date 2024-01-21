package io.bhex.broker.quote.config;

import io.bhex.broker.quote.util.BrokerAuthUtil;
import io.grpc.*;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executor;

/**
 * @author wangshouchao
 */
@Slf4j
public class RouteAuthCredentials implements CallCredentials {

    private final String routeKey;
    private final Long orgId;
    private static final Metadata.Key<String> API_KEY_META = Metadata.Key.of("ApiKey", Metadata.ASCII_STRING_MARSHALLER);

    public RouteAuthCredentials(String routeKey, Long orgId) {
        this.routeKey = routeKey;
        this.orgId = orgId;
    }

    @Override
    public void applyRequestMetadata(MethodDescriptor<?, ?> method, Attributes attrs, Executor appExecutor, MetadataApplier applier) {
        appExecutor.execute(() -> {
            try {
                BrokerAuthUtil.BrokerApiAuth brokerAuth = BrokerAuthUtil.getBrokerAuth(orgId);
                if (brokerAuth == null) {
                    log.warn("unknown orgId:{}", orgId);
                    applier.fail(Status.INTERNAL.withDescription("unknown orgId:" + orgId));
                    return;
                }
                Metadata headers = new Metadata();
                Metadata.Key<String> rKey = Metadata.Key.of("route-channel", Metadata.ASCII_STRING_MARSHALLER);
                headers.put(rKey, routeKey);
                Metadata.Key<String> authKey = Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);
                headers.put(authKey, brokerAuth.getAuthData());
                headers.put(API_KEY_META, brokerAuth.getApiKey());
                applier.apply(headers);
            } catch (Throwable e) {
                applier.fail(Status.UNAUTHENTICATED.withCause(e));
            }
        });
    }


    @Override
    public void thisUsesUnstableApi() {

    }
}
