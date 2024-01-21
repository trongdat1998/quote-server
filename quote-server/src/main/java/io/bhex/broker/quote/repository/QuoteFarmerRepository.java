package io.bhex.broker.quote.repository;

import io.bhex.base.quote.IndexConfigList;
import io.bhex.base.quote.IndexConfigRequest;
import io.bhex.base.quote.ThirdQuoteServiceGrpc;
import io.bhex.broker.quote.config.GrpcRunner;
import io.bhex.broker.quote.util.BaseReqUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class QuoteFarmerRepository {
    private final GrpcRunner grpcRunner;

    @Autowired
    public QuoteFarmerRepository(GrpcRunner grpcRunner) {
        this.grpcRunner = grpcRunner;
    }

    public IndexConfigList getIndexConfigList(Long orgId) {
        ThirdQuoteServiceGrpc.ThirdQuoteServiceBlockingStub stub = grpcRunner.thirdQuoteServiceBlockingStub(orgId);
        return stub.indexConfigList(IndexConfigRequest.newBuilder().setBaseRequest(BaseReqUtil.getBaseRequest(orgId)).build());
    }
}
