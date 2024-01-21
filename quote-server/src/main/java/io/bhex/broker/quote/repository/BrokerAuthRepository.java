package io.bhex.broker.quote.repository;

import io.bhex.broker.grpc.common.Header;
import io.bhex.broker.grpc.gateway.*;
import io.bhex.broker.quote.config.ProxyGrpcRunner;
import io.bhex.broker.quote.util.BrokerAuthUtil;
import io.grpc.ManagedChannel;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author wangsc
 * @description 专门用于获取broker认证
 * @date 2020-06-30 18:56
 */
@Repository
@ConditionalOnProperty(name = "bhex.proxy", havingValue = "true")
public class BrokerAuthRepository {
    @Resource
    private ProxyGrpcRunner grpcRunner;
    private final long CONNECT_TIMEOUT = 5000L;

    /**
     * 使用gateway时调用获取当前所有认证的orgId
     *
     * @return
     */
    public List<Long> getALLAuthOrgIds() {
        ManagedChannel channel = grpcRunner.borrowBrokerChannel();
        BrokerAuthServiceGrpc.BrokerAuthServiceBlockingStub stub = BrokerAuthServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
        GetALLAuthOrgIdsResponse response = stub.getALLAuthOrgIds(GetALLAuthOrgIdsRequest.newBuilder().build());
        return response.getRet() == 0 ? response.getOrgIdsList() : new ArrayList<>();
    }

    /**
     * 使用gateway时调用获取当前认证信息
     *
     * @param orgId
     * @return
     */
    public BrokerAuthUtil.BrokerApiAuth getBrokerAuthByOrgId(Long orgId) {
        ManagedChannel channel = grpcRunner.borrowBrokerChannel();
        BrokerAuthServiceGrpc.BrokerAuthServiceBlockingStub stub = BrokerAuthServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
        GetBrokerAuthByOrgIdResponse response = stub.getBrokerAuthByOrgId(GetBrokerAuthByOrgIdRequest
                .newBuilder()
                .setHeader(Header.newBuilder().setOrgId(orgId).build())
                .build());
        if (response.getRet() == 0) {
            GetBrokerAuthByOrgIdResponse.BrokerOrgAuth brokerOrgAuth = response.getBrokerOrgAuth();
            return BrokerAuthUtil.BrokerApiAuth.builder().apiKey(brokerOrgAuth.getApiKey()).authData(brokerOrgAuth.getAuthData()).refreshTime(brokerOrgAuth.getRefreshTime()).build();
        } else {
            return null;
        }
    }

}
