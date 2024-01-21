package io.bhex.broker.quote.repository;

import io.bhex.base.match.Ticket;
import io.bhex.base.quote.Depth;
import io.bhex.base.quote.GetDepthReply;
import io.bhex.base.quote.GetExchangeRateReply;
import io.bhex.base.quote.GetExchangeRateRequest;
import io.bhex.base.quote.GetIndexLatestKLineRequest;
import io.bhex.base.quote.GetIndicesReply;
import io.bhex.base.quote.GetIndicesRequest;
import io.bhex.base.quote.GetKLineReply;
import io.bhex.base.quote.GetKLineRequest;
import io.bhex.base.quote.GetLatestKLineRequest;
import io.bhex.base.quote.GetQuoteRequest;
import io.bhex.base.quote.GetRealtimeReply;
import io.bhex.base.quote.GetTicketReply;
import io.bhex.base.quote.Index;
import io.bhex.base.quote.KLine;
import io.bhex.base.quote.QuoteServiceGrpc;
import io.bhex.base.quote.Rate;
import io.bhex.base.quote.Realtime;
import io.bhex.broker.quote.config.GrpcRunner;
import io.bhex.broker.quote.data.QuoteIndex;
import io.bhex.broker.quote.util.BaseReqUtil;
import io.bhex.ex.quote.PartitionListReply;
import io.bhex.ex.quote.PartitionListRequest;
import io.bhex.ex.quote.PartitionMarketInfoReply;
import io.bhex.ex.quote.PartitionMarketInfoRequest;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public class DataServiceRepository {
    private final GrpcRunner grpcRunner;

    @Autowired
    public DataServiceRepository(GrpcRunner grpcRunner) {
        this.grpcRunner = grpcRunner;
    }

    public Realtime getRealTime(Long exchangeId, String symbol, Long orgId) {
        QuoteServiceGrpc.QuoteServiceBlockingStub stub = grpcRunner.quoteServiceBlockingStub(orgId);
        GetQuoteRequest request = GetQuoteRequest.newBuilder()
            .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
            .setExchangeId(exchangeId)
            .setSymbol(symbol)
            .build();
        GetRealtimeReply reply = stub.getRealtime(request);
        if (reply.getRealtimeCount() == 0) {
            return null;
        }
        return reply.getRealtimeList().iterator().next();
    }

    public List<Ticket> getTrades(Long exchangeId, String symbol, int sinceId, int tradeCachedSize, Long orgId) {
        QuoteServiceGrpc.QuoteServiceBlockingStub stub = grpcRunner.quoteServiceBlockingStub(orgId);
        GetQuoteRequest request = GetQuoteRequest.newBuilder()
            .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
            .setExchangeId(exchangeId)
            .setSymbol(symbol)
            .setSinceId(sinceId)
            .setLimitCount(tradeCachedSize)
            .build();
        GetTicketReply reply = stub.getTicket(request);
        return reply.getTicketList();
    }

    public List<KLine> getHistoryKlineList(QuoteIndex.KlineIndex index, Long orgId) {
        return getHistoryKlineList(index.getExchangeId(), index.getSymbol(), index.getKlineType(), index.getFrom(),
            index.getTo(), orgId);
    }

    public List<KLine> getHistoryKlineList(Long exchangeId, String symbol, String interval, long from, long to, Long orgId) {
        QuoteServiceGrpc.QuoteServiceBlockingStub stub = grpcRunner.quoteServiceBlockingStub(orgId);
        GetKLineRequest request = GetKLineRequest.newBuilder()
            .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
            .setExchangeId(exchangeId)
            .setSymbol(symbol)
            .setInterval(interval)
            .setStartTime(from)
            .setEndTime(to)
            .build();
        GetKLineReply reply = stub.getHistoryKLine(request);
        return reply.getKlineList();
    }

    public List<KLine> getKlineList(Long exchangeId, String symbol, String interval, int klineCacheSize, Long orgId) {
        QuoteServiceGrpc.QuoteServiceBlockingStub stub = grpcRunner.quoteServiceBlockingStub(orgId);
        GetLatestKLineRequest request = GetLatestKLineRequest.newBuilder()
            .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
            .setExchangeId(exchangeId)
            .setSymbol(symbol)
            .setInterval(interval)
            .setLimitCount(klineCacheSize)
            .build();
        GetKLineReply kLineReply = stub.getLatestKLine(request);
        return kLineReply.getKlineList();
    }

    public List<KLine> getIndexKlineList(String symbol, String interval, int klineCacheSize, Long orgId) {
        QuoteServiceGrpc.QuoteServiceBlockingStub stub = grpcRunner.quoteServiceBlockingStub(orgId);
        GetIndexLatestKLineRequest request = GetIndexLatestKLineRequest.newBuilder()
            .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
            .setSymbol(symbol)
            .setInterval(interval)
            .setLimitCount(klineCacheSize)
            .build();
        GetKLineReply kLineReply = stub.getIndexLatestKLine(request);
        return kLineReply.getKlineList();
    }

    public Depth getPartialDepth(Long exchangeId, String symbol, int count, Long orgId) {
        QuoteServiceGrpc.QuoteServiceBlockingStub stub = grpcRunner.quoteServiceBlockingStub(orgId);
        GetQuoteRequest request = GetQuoteRequest.newBuilder()
            .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
            .setExchangeId(exchangeId)
            .setSymbol(symbol)
            .setLimitCount(count)
            .build();
        GetDepthReply reply = stub.getPartialDepth(request);
        if (reply.getDepthCount() == 0) {
            return null;
        }
        return reply.getDepthList().iterator().next();
    }

    public Depth getPartialDepth(Long exchangeId, String symbol, Integer dumpScale, int count, Long orgId) {
        QuoteServiceGrpc.QuoteServiceBlockingStub stub = grpcRunner.quoteServiceBlockingStub(orgId);
        GetQuoteRequest request = GetQuoteRequest.newBuilder()
            .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
            .setExchangeId(exchangeId)
            .setSymbol(symbol)
            .setLimitCount(count)
            .setDumpScale(dumpScale)
            .build();
        GetDepthReply reply = stub.getPartialDepth(request);
        if (reply.getDepthCount() == 0) {
            return null;
        }
        return reply.getDepthList().iterator().next();
    }

    public Map<String, Index> getIndices(List<String> indexTokenList, Long orgId) {
        QuoteServiceGrpc.QuoteServiceBlockingStub stub = grpcRunner.quoteServiceBlockingStub(orgId);
        GetIndicesRequest request = GetIndicesRequest.newBuilder()
            .setBaseRequest(BaseReqUtil.getBaseRequest(orgId))
            .addAllSymbols(indexTokenList)
            .build();
        GetIndicesReply reply = stub.getIndices(request);
        return reply.getIndicesMapMap();
    }

    public Map<String, Rate> getRate(Long exchangeId, List<String> tokenList, Long orgId) {
        QuoteServiceGrpc.QuoteServiceBlockingStub stub = grpcRunner.quoteServiceBlockingStub(orgId);
        GetExchangeRateRequest.Builder requestBuilder = GetExchangeRateRequest.newBuilder();
        requestBuilder.setBaseRequest(BaseReqUtil.getBaseRequest(orgId));
        requestBuilder.setExchangeId(exchangeId);
        if (CollectionUtils.isNotEmpty(tokenList)) {
            for (String token : tokenList) {
                requestBuilder.addTokens(token);
            }
        }
        GetExchangeRateReply reply = stub.getRatesV3(requestBuilder.build());
        return reply.getExchangeRate().getRateMapMap();
    }

    public PartitionListReply getPartitionNameList(Long orgId) {
        //分区理论上应该迁移,现使用查询后直连
        PartitionListRequest request = PartitionListRequest.newBuilder().setOrgId(orgId).build();
        return grpcRunner.quoteMarketServiceBlockingStub(orgId).getPartitionList(request);
    }

    public PartitionMarketInfoReply getPartitionMarketInfo(String partitionName, Long orgId) {
        //分区理论上应该迁移,现使用查询后直连
        PartitionMarketInfoRequest request = PartitionMarketInfoRequest.newBuilder().setPartitionName(partitionName).setOrgId(orgId).build();
        return grpcRunner.quoteMarketServiceBlockingStub(orgId).getPartitionMarketInfo(request);
    }

    public PartitionMarketInfoReply getPartitionMarketInfo0(String appName, String partitionName, Long orgId) {
        //分区理论上应该迁移,现使用查询后直连
        PartitionMarketInfoRequest request = PartitionMarketInfoRequest.newBuilder().setPartitionName(partitionName).setAppName(appName).setOrgId(orgId).build();
        return grpcRunner.quoteMarketServiceBlockingStub(orgId).getPartitionMarketInfo0(request);
    }
}
