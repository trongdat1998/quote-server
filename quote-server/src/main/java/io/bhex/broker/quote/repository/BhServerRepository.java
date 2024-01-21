package io.bhex.broker.quote.repository;

import io.bhex.base.account.FuturesServerGrpc;
import io.bhex.base.account.GetOptionIndexReq;
import io.bhex.base.account.GetOptionIndexTokenListReply;
import io.bhex.base.account.GetTotalPositionReply;
import io.bhex.base.account.GetTotalPositionRequest;
import io.bhex.base.account.OptionServerGrpc;
import io.bhex.base.quote.IndexConfig;
import io.bhex.base.token.GetSymbolListRequest;
import io.bhex.base.token.GetSymbolMapReply;
import io.bhex.base.token.GetSymbolMapRequest;
import io.bhex.base.token.SymbolDetail;
import io.bhex.base.token.SymbolList;
import io.bhex.base.token.SymbolServiceGrpc;
import io.bhex.broker.quote.config.GrpcRunner;
import io.bhex.broker.quote.util.BaseReqUtil;
import io.bhex.broker.quote.util.KeyUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

@Repository
@Slf4j
public class BhServerRepository {

    private final GrpcRunner grpcRunner;
    private final long CONNECT_TIMEOUT = 5000L;

    private ConcurrentMap<String, BigDecimal> positionMap = new ConcurrentHashMap<>();
    private ConcurrentMap<String, IndexConfig> indexConfigMap = new ConcurrentHashMap<>();

    @Autowired
    public BhServerRepository(GrpcRunner grpcRunner) {
        this.grpcRunner = grpcRunner;
    }

    public void updateIndexConfig(List<IndexConfig> indexConfigs) {
        for (IndexConfig indexConfig : indexConfigs) {
            indexConfigMap.put(indexConfig.getName(), indexConfig);
        }
    }

    public IndexConfig getIndexConfig(String symbol) {
        return indexConfigMap.get(symbol);
    }

    public BigDecimal getPosition(Long exchangeId, String symbolId) {
        String key = KeyUtils.getPositionKey(exchangeId, symbolId);
        BigDecimal res = positionMap.get(key);
        if (res == null) {
            return BigDecimal.ZERO;
        }
        return res;
    }

    public void updatePosition(Long exchangeId, Map<String, String> posMap) {
        ConcurrentMap<String, BigDecimal> positionMap = new ConcurrentHashMap<>();
        for (Map.Entry<String, String> stringStringEntry : posMap.entrySet()) {
            try {
                String key = KeyUtils.getPositionKey(exchangeId, stringStringEntry.getKey());
                positionMap.put(key, new BigDecimal(stringStringEntry.getValue()));
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        this.positionMap.putAll(positionMap);
    }

    public List<String> getIndexTokenList(Long orgId) {
        OptionServerGrpc.OptionServerBlockingStub stub = grpcRunner.optionServerBlockingStub(orgId)
            .withDeadlineAfter(CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
        GetOptionIndexTokenListReply reply = stub.getOptionIndexTokenList(GetOptionIndexReq.newBuilder()
            .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
            .build());
        return reply.getIndexTokenList();
    }

    public List<SymbolDetail> getSymbolDetailList(Long exchangeId, Long orgId) {
        SymbolServiceGrpc.SymbolServiceBlockingStub stub = grpcRunner.symbolServiceBlockingStub(orgId)
            .withDeadlineAfter(CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
        SymbolList response = stub.getSymbolList(GetSymbolListRequest.newBuilder()
            .setOrgId(orgId)
            .setExchangeId(exchangeId)
            .build());
        return response.getSymbolDetailsList();
    }

    public Map<Long, SymbolList> getSymbolMap(List<Long> exchangeIds, Long orgId) {
        SymbolServiceGrpc.SymbolServiceBlockingStub stub = grpcRunner.symbolServiceBlockingStub(orgId)
            .withDeadlineAfter(CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
        GetSymbolMapReply response = stub.getSymbolMap(GetSymbolMapRequest.newBuilder()
            .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
            .addAllExchangeIds(exchangeIds)
            .build());
        return response.getSymbolMapMap();
    }

    public Map<String, String> getTotalPosition(Long exchangeId, Long orgId) {
        FuturesServerGrpc.FuturesServerBlockingStub stub = grpcRunner.futuresServerBlockingStub(orgId);
        GetTotalPositionReply reply = stub.getTotalPosition(GetTotalPositionRequest.newBuilder()
            .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
            .setExchangeId(exchangeId)
            .build());
        return reply.getTokenPositionMap();
    }

}
