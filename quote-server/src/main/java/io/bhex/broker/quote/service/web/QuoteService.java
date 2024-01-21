package io.bhex.broker.quote.service.web;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.bhex.base.proto.Decimal;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.Index;
import io.bhex.base.quote.IndexConfig;
import io.bhex.base.quote.KLine;
import io.bhex.base.quote.Rate;
import io.bhex.base.token.TokenCategory;
import io.bhex.broker.grpc.basic.Symbol;
import io.bhex.broker.quote.common.KlineTypes;
import io.bhex.broker.quote.data.BookTicker;
import io.bhex.broker.quote.data.QuoteIndex;
import io.bhex.broker.quote.data.Symbols;
import io.bhex.broker.quote.data.dto.DepthDTO;
import io.bhex.broker.quote.data.dto.IndexConfigDTO;
import io.bhex.broker.quote.data.dto.KlineItemDTO;
import io.bhex.broker.quote.data.dto.MarkPriceDTO;
import io.bhex.broker.quote.data.dto.RateDTO;
import io.bhex.broker.quote.data.dto.RealTimeDTO;
import io.bhex.broker.quote.data.dto.TicketDTO;
import io.bhex.broker.quote.data.dto.api.TickerDTO;
import io.bhex.broker.quote.enums.ErrorCodeEnum;
import io.bhex.broker.quote.enums.RealtimeIntervalEnum;
import io.bhex.broker.quote.exception.BizException;
import io.bhex.broker.quote.job.PlatformTask;
import io.bhex.broker.quote.listener.BrokerListener;
import io.bhex.broker.quote.listener.DepthListener;
import io.bhex.broker.quote.listener.IndexKlineListener;
import io.bhex.broker.quote.listener.KlineListener;
import io.bhex.broker.quote.listener.MergedDepthListener;
import io.bhex.broker.quote.listener.RealTimeListener;
import io.bhex.broker.quote.listener.TopNListener;
import io.bhex.broker.quote.listener.TradeListener;
import io.bhex.broker.quote.repository.BhServerRepository;
import io.bhex.broker.quote.repository.DataServiceRepository;
import io.bhex.broker.quote.repository.QuoteRepository;
import io.bhex.broker.quote.repository.SymbolRepository;
import io.bhex.broker.quote.util.BeanUtils;
import io.bhex.broker.quote.util.BrokerAuthUtil;
import io.bhex.broker.quote.util.FilterHistoryKlineUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
@Slf4j
public class QuoteService {
    private final ApplicationContext context;
    private final SymbolRepository symbolRepository;
    private final PlatformTask platformTask;
    private final DataServiceRepository dataServiceRepository;
    private final QuoteRepository quoteRepository;
    private final BhServerRepository bhServerRepository;

    private LoadingCache<QuoteIndex.KlineIndex, List<KLine>> historyKlineCache = CacheBuilder.newBuilder()
        .expireAfterAccess(2, TimeUnit.MINUTES)
        .expireAfterWrite(2, TimeUnit.MINUTES)
        .build(new CacheLoader<QuoteIndex.KlineIndex, List<KLine>>() {
            @Override
            public List<KLine> load(QuoteIndex.KlineIndex klineIndex) throws Exception {
                //非gateway模式时，获取的orgId是0，后期平台orgId生效时再使用所有的orgId合并结果处理
                Long orgId = BrokerAuthUtil.getOrgIdSet().stream().findFirst().orElse(0L);
                return dataServiceRepository.getHistoryKlineList(klineIndex, orgId);
            }
        });

    @Autowired
    public QuoteService(
        SymbolRepository symbolRepository,
        DataServiceRepository dataServiceRepository,
        ApplicationContext context,
        PlatformTask platformTask,
        QuoteRepository quoteRepository,
        BhServerRepository bhServerRepository) {
        this.symbolRepository = symbolRepository;
        this.context = context;
        this.platformTask = platformTask;
        this.dataServiceRepository = dataServiceRepository;
        this.quoteRepository = quoteRepository;
        this.bhServerRepository = bhServerRepository;
    }

    /**
     * rate cache
     */
    private ConcurrentMap<Long, CopyOnWriteArraySet<String>> rateTokenSetMap = new ConcurrentHashMap<>();
    private ConcurrentMap<Long, ConcurrentMap<String, Rate>> rateCacheMap = new ConcurrentHashMap<>();

    public MarkPriceDTO getMarkPrice(String host, String symbol) throws BizException {
        Long orgId = symbolRepository.getOrgIdByDomain(host);
        Long exchangeId = symbolRepository.getExchangeIdBySymbolAndOrgId(symbol, orgId);
        return quoteRepository.getMarkPriceDTO(exchangeId, symbol);
    }

    public IndexConfigDTO getIndexConfig(String name) throws BizException {
        IndexConfig indexConfig = bhServerRepository.getIndexConfig(name);
        if (Objects.isNull(indexConfig)) {
            throw new BizException(ErrorCodeEnum.INDEX_NAME_ERROR);
        }
        return IndexConfigDTO.parse(indexConfig);
    }

    public List<MarkPriceDTO> getMarkPriceDTOList(Set<Symbols> symbolSet) {
        List<MarkPriceDTO> markPriceDTOList = new ArrayList<>();
        for (Symbols symbols : symbolSet) {
            Long exchangeId = symbols.getExchangeId();
            String symbolId = symbols.getSymbol();
            MarkPriceDTO markPriceDTO = quoteRepository.getMarkPriceDTO(exchangeId, symbolId);
            if (Objects.nonNull(markPriceDTO)) {
                markPriceDTOList.add(markPriceDTO);
            }
        }
        return markPriceDTOList;
    }

    public List<RealTimeDTO> getBrokerTickers(String host, int type, RealtimeIntervalEnum realtimeIntervalEnum) throws BizException {
        Long orgId = symbolRepository.getOrgIdByDomain(host);
        try {
            BrokerListener brokerListener = context.getBean(BeanUtils.getBrokerBeanName(orgId, type, realtimeIntervalEnum.getInterval()), BrokerListener.class);
            return brokerListener.getCurrentTickers();
        } catch (NoSuchBeanDefinitionException e) {
            log.error("No brokerListener find broker [{}]", orgId);
            return Collections.emptyList();
        }
    }

    public List<RealTimeDTO> getTopN(String host, int type, int limit, RealtimeIntervalEnum realtimeIntervalEnum) throws BizException {
        Long orgId = symbolRepository.getOrgIdByDomain(host);
        try {
            TopNListener topNListener = context.getBean(BeanUtils.getBrokerTopNBeanName(orgId, type, realtimeIntervalEnum.getInterval()), TopNListener.class);
            return topNListener.getTopN(limit);
        } catch (NoSuchBeanDefinitionException e) {
            log.error("No topNListener find broker [{}]", orgId);
            return Collections.emptyList();
        }
    }

    public List<RateDTO> getRates(String host, List<String> tokenList, List<String> legalCoinList) throws BizException {
        Long orgId = symbolRepository.getOrgIdByDomain(host);
        Long exchangeId = symbolRepository.getFirstExchangeIdByOrgId(orgId);
        return getRates(exchangeId, tokenList, legalCoinList, orgId);
    }

    @Scheduled(initialDelay = 30 * 1000L, fixedDelay = 30 * 1000L)
    public void refreshRateCache() {
        try {
            //TODO 非gateway模式时，获取的orgId是0，后期平台orgId生效时再使用所有的orgId合并结果处理
            Long orgId = BrokerAuthUtil.getOrgIdSet().stream().findFirst().orElse(0L);
            ConcurrentMap<Long, CopyOnWriteArraySet<String>> rateTokenSetMap = this.rateTokenSetMap;
            for (Map.Entry<Long, CopyOnWriteArraySet<String>> longCopyOnWriteArraySetEntry : rateTokenSetMap.entrySet()) {
                Long exchangeId = longCopyOnWriteArraySetEntry.getKey();
                CopyOnWriteArraySet<String> tokenSet = longCopyOnWriteArraySetEntry.getValue();
                Map<String, Rate> rateMap = dataServiceRepository.getRate(exchangeId, new ArrayList<>(tokenSet), orgId);
                rateCacheMap.put(exchangeId, new ConcurrentHashMap<>(rateMap));
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public List<RateDTO> getRates(Long exchangeId, List<String> tokenList, List<String> legalCoinList, Long orgId) {
        ConcurrentMap<String, Rate> existTokenMap = rateCacheMap.get(exchangeId);
        if (existTokenMap == null) {
            existTokenMap = new ConcurrentHashMap<>();
            rateCacheMap.put(exchangeId, existTokenMap);
        }
        List<String> notCachedTokenList = new ArrayList<>();
        List<RateDTO> resultList = new ArrayList<>();
        for (String token : tokenList) {
            Rate rate = existTokenMap.get(token);
            if (Objects.isNull(rate)) {
                notCachedTokenList.add(token);
            } else {
                Map<String, Decimal> rateMap = rate.getRatesMap();
                Map<String, BigDecimal> rateDTORateMap = new HashMap<>();
                for (String legalCoin : legalCoinList) {
                    Decimal rateValue = rateMap.get(legalCoin);
                    if (Objects.nonNull(rateValue)) {
                        rateDTORateMap.put(legalCoin, DecimalUtil.toBigDecimal(rateValue));
                    }
                }
                resultList.add(RateDTO.builder()
                    .rates(rateDTORateMap)
                    .token(token)
                    .build());
            }
        }

        if (CollectionUtils.isNotEmpty(notCachedTokenList)) {
            CopyOnWriteArraySet<String> tokenSet = rateTokenSetMap.get(exchangeId);
            if (Objects.isNull(tokenSet)) {
                tokenSet = new CopyOnWriteArraySet<>();
                rateTokenSetMap.put(exchangeId, tokenSet);
            }
            Map<String, Rate> rateMap = dataServiceRepository.getRate(exchangeId, notCachedTokenList, orgId);
            for (Map.Entry<String, Rate> stringRateEntry : rateMap.entrySet()) {
                String token = stringRateEntry.getKey();
                tokenSet.add(token);
                Rate rate = stringRateEntry.getValue();
                Map<String, Decimal> rateMapMap = rate.getRatesMap();
                Map<String, BigDecimal> rateDTORateMap = new HashMap<>();
                for (String legalCoin : legalCoinList) {
                    Decimal rateValue = rateMapMap.get(legalCoin);
                    if (Objects.nonNull(rateValue)) {
                        rateDTORateMap.put(legalCoin, DecimalUtil.toBigDecimal(rateValue));
                    }
                }
                resultList.add(RateDTO.builder()
                    .rates(rateDTORateMap)
                    .token(token)
                    .build());
            }
        }

        return resultList;
    }

    public Map<String, Index> getIndexMap(Set<String> symbolSet) {
        if (CollectionUtils.isEmpty(symbolSet)) {
            return platformTask.getAllIndices();
        } else {
            return platformTask.getIndexMapBySymbolSet(symbolSet);
        }
    }

    /**
     * 获取原始深度
     */
    public DepthDTO getDepth(Long exchangeId, String symbol) throws BizException {
        commonCheck(exchangeId, symbol);
        DepthDTO depthDTO = getDepthSnapshot(exchangeId, symbol);
        depthDTO.setExchangeId(exchangeId);
        return depthDTO;
    }

    private DepthDTO getDepthSnapshot(Long exchangeId, String symbol) {
        try {
            exchangeId = symbolRepository.getSharedExchangeId(exchangeId, symbol);
            DepthListener depthListener = context
                .getBean(BeanUtils.getDepthBeanName(exchangeId, symbol), DepthListener.class);
            DepthDTO depthDTOS = depthListener.getSnapshotData();
            if (Objects.nonNull(depthDTOS)) {
                return depthDTOS;
            }
            return DepthDTO.getDefaultDepthDTO(exchangeId, symbol);
        } catch (NoSuchBeanDefinitionException e) {
            return DepthDTO.getDefaultDepthDTO(exchangeId, symbol);
        }
    }

    public DepthDTO getDepth(Long exchangeId, String symbol, int scale) throws BizException {
        commonCheck(exchangeId, symbol);
        try {
            exchangeId = symbolRepository.getSharedExchangeId(exchangeId, symbol);
            MergedDepthListener mergedDepthListener = (MergedDepthListener) context
                .getBean(BeanUtils.getDepthBeanName(exchangeId, symbol, scale));
            DepthDTO depthDTOS = mergedDepthListener.getSnapshotData();
            if (Objects.nonNull(depthDTOS)) {
                return depthDTOS;
            }
            return DepthDTO.getDefaultDepthDTO(exchangeId, symbol);
        } catch (NoSuchBeanDefinitionException e) {
            return DepthDTO.getDefaultDepthDTO(exchangeId, symbol);
        }
    }

    public DepthDTO getDepth(String host, String symbol) throws BizException {
        Long orgId = symbolRepository.getOrgIdByDomain(host);
        String paramSymbol = symbol;
        symbol = symbolRepository.getRealSymbolId(orgId, paramSymbol);
        symbolCheck(symbol);
        Long exchangeId = symbolRepository.getExchangeIdBySymbolAndOrgId(symbol, orgId);
        exchangeIdCheck(exchangeId);
        DepthDTO depthDTO = getDepth(exchangeId, symbol);
        if (!StringUtils.equals(paramSymbol, symbol)) {
            depthDTO.setSymbol(paramSymbol);
        }
        return depthDTO;
    }

    public DepthDTO getDepth(String host, String symbol, Integer dumpScale) throws BizException {
        Long orgId = symbolRepository.getOrgIdByDomain(host);
        String paramSymbol = symbol;
        symbol = symbolRepository.getRealSymbolId(orgId, paramSymbol);
        symbolCheck(symbol);
        Long exchangeId = symbolRepository.getExchangeIdBySymbolAndOrgId(symbol, orgId);
        exchangeIdCheck(exchangeId);
        if (Objects.isNull(dumpScale)) {
            dumpScale = symbolRepository.getMaxScale(symbol);
        }
        DepthDTO depthDTO = getDepth(exchangeId, symbol, dumpScale);
        if (!StringUtils.equals(paramSymbol, symbol)) {
            depthDTO.setSymbol(paramSymbol);
        }
        return depthDTO;
    }

    public TickerDTO getTicker(String host, String symbol, RealtimeIntervalEnum realtimeIntervalEnum) throws BizException {
        Long orgId = symbolRepository.getOrgIdByDomain(host);
        String paramSymbol = symbol;
        symbol = symbolRepository.getRealSymbolId(orgId, paramSymbol);
        Long exchangeId = symbolRepository.getExchangeIdBySymbolAndOrgId(symbol, orgId);
        commonCheck(exchangeId, symbol);
        RealTimeDTO realtime = getRealTime(exchangeId, symbol, realtimeIntervalEnum);
        DepthDTO depth = getDepthSnapshot(exchangeId, symbol);
        TickerDTO tickerDTO = TickerDTO.parse(realtime, depth);
        if (!StringUtils.equals(paramSymbol, symbol)) {
            tickerDTO.setSymbol(paramSymbol);
        }
        Symbol symbolObj = symbolRepository.getSymbol(symbol);
        if (symbolObj.getCategory() == TokenCategory.FUTURE_CATEGORY_VALUE) {
            BigDecimal openInterest = bhServerRepository.getPosition(exchangeId, symbol);
            tickerDTO.setOpenInterest(openInterest);
        }
        return tickerDTO;
    }

    /**
     * 获取原始行情
     */
    public RealTimeDTO getRealTime(Long exchangeId, String symbol, RealtimeIntervalEnum realtimeIntervalEnum) throws BizException {
        commonCheck(exchangeId, symbol);
        RealTimeDTO realtimeDto;
        try {
            exchangeId = symbolRepository.getSharedExchangeId(exchangeId, symbol);
            RealTimeListener dataListener = (RealTimeListener) context
                    .getBean(BeanUtils.getRealTimeBeanName(exchangeId, symbol, realtimeIntervalEnum.getInterval()));
            RealTimeDTO realTimeDTOS = dataListener.getSnapshotData();
            if (Objects.isNull(realTimeDTOS)) {
                realtimeDto = RealTimeDTO.getDefaultRealTime(exchangeId, symbol);
            } else {
                realtimeDto = realTimeDTOS;
            }
        } catch (NoSuchBeanDefinitionException e) {
            realtimeDto = RealTimeDTO.getDefaultRealTime(exchangeId, symbol);
        }
        return realtimeDto;
    }

    public RealTimeDTO getRealTime(String host, String symbol, RealtimeIntervalEnum realtimeIntervalEnum) throws BizException {
        Long orgId = symbolRepository.getOrgIdByDomain(host);
        String paramSymbol = symbol;
        symbol = symbolRepository.getRealSymbolId(orgId, symbol);
        Long exchangeId = symbolRepository.getExchangeIdBySymbolAndOrgId(symbol, orgId);
        RealTimeDTO realTimeDTO = getRealTime(exchangeId, symbol, realtimeIntervalEnum);
        RealTimeDTO result = new RealTimeDTO();
        result.cloneData(realTimeDTO);
        result.setSymbol(paramSymbol);
        return realTimeDTO;
    }

    /**
     * 一个交易所的所有币对
     *
     * @param exchangeId 交易所id
     */
    private List<RealTimeDTO> getRealTimeList(Long orgId, Long exchangeId, RealtimeIntervalEnum realtimeIntervalEnum) throws BizException {
        exchangeIdCheck(exchangeId);
        Set<String> symbols = symbolRepository.getAllSymbolsByExchangeId(exchangeId);
        List<RealTimeDTO> realTimeList = new ArrayList<>();
        for (String symbol : symbols) {
            try {
                String symbolName = symbolRepository.getSymbolName(orgId, symbol);
                RealTimeDTO realTimeDTO = getRealTime(exchangeId, symbol, realtimeIntervalEnum);
                RealTimeDTO result = new RealTimeDTO();
                result.cloneData(realTimeDTO);
                result.setSymbol(symbolName);
                realTimeList.add(result);
            } catch (Exception ignored) {
                // 异常说明没有对应symbol，只返回有的就可以了
            }
        }
        return realTimeList;
    }

    public List<RealTimeDTO> getRealTimeList(String host, RealtimeIntervalEnum realtimeIntervalEnum) throws BizException {
        Long orgId = symbolRepository.getOrgIdByDomain(host);
        Long exchangeId = symbolRepository.getFirstExchangeIdByOrgId(orgId);
        return getRealTimeList(orgId, exchangeId, realtimeIntervalEnum);
    }

    public List<RealTimeDTO> getFuturesRealTimeList(String host, RealtimeIntervalEnum realtimeIntervalEnum) throws BizException {
        Long orgId = symbolRepository.getOrgIdByDomain(host);
        Long exchangeId = symbolRepository.getFirstExchangeIdByOrgId(orgId);
        return getFuturesRealTimeList(orgId, exchangeId, realtimeIntervalEnum);
    }

    public List<RealTimeDTO> getFuturesRealTimeList(Long orgId, Long exchangeId, RealtimeIntervalEnum realtimeIntervalEnum) throws BizException {
        exchangeIdCheck(exchangeId);
        Set<String> symbols = symbolRepository.getFuturesAllSymbolsByExchangeId(exchangeId);
        List<RealTimeDTO> realTimeList = new ArrayList<>();
        for (String symbol : symbols) {
            try {
                String symbolName = symbolRepository.getSymbolName(orgId, symbol);
                RealTimeDTO realTimeDTO = getRealTime(exchangeId, symbol, realtimeIntervalEnum);
                RealTimeDTO result = new RealTimeDTO();
                result.cloneData(realTimeDTO);
                BigDecimal openInterest = bhServerRepository.getPosition(exchangeId, symbol);
                result.setOpenInterest(openInterest);
                result.setSymbol(symbolName);
                realTimeList.add(result);
            } catch (Exception ignored) {
                // 异常说明没有对应symbol，只返回有的就可以了
            }
        }
        return realTimeList;
    }

    public List<RealTimeDTO> getOptionRealTimeList(String host, RealtimeIntervalEnum realtimeIntervalEnum) throws BizException {
        Long orgId = symbolRepository.getOrgIdByDomain(host);
        Long exchangeId = symbolRepository.getFirstExchangeIdByOrgId(orgId);
        return getOptionRealTimeList(orgId, exchangeId, realtimeIntervalEnum);
    }

    public List<RealTimeDTO> getOptionRealTimeList(Long orgId, Long exchangeId, RealtimeIntervalEnum realtimeIntervalEnum) throws BizException {
        exchangeIdCheck(exchangeId);
        Set<String> symbols = symbolRepository.getOptionAllSymbolByExchangeId(exchangeId);
        List<RealTimeDTO> realTimeList = new ArrayList<>();
        for (String symbol : symbols) {
            try {
                String symbolName = symbolRepository.getSymbolName(orgId, symbol);
                RealTimeDTO realTimeDTO = getRealTime(exchangeId, symbol, realtimeIntervalEnum);
                RealTimeDTO result = new RealTimeDTO();
                result.cloneData(realTimeDTO);
                result.setSymbol(symbolName);
                realTimeList.add(result);
            } catch (Exception ignored) {
                // 异常说明没有对应symbol，只返回有的就可以了
            }
        }
        return realTimeList;
    }

    /**
     * 获取买一卖一价格
     */
    private BookTicker getBookTicker(Long exchangeId, String symbol) throws BizException {
        commonCheck(exchangeId, symbol);
        DepthDTO depthDTO = getDepthSnapshot(exchangeId, symbol);
        return BookTicker.parse(depthDTO);
    }

    public BookTicker getBookTicker(String host, String symbol) throws BizException {
        Long orgId = symbolRepository.getOrgIdByDomain(host);
        String paramSymbol = symbol;
        symbol = symbolRepository.getRealSymbolId(orgId, symbol);
        Long exchangeId = symbolRepository.getExchangeIdBySymbolAndOrgId(symbol, orgId);
        BookTicker bookTicker = getBookTicker(exchangeId, symbol);
        bookTicker.setSymbol(paramSymbol);
        return bookTicker;
    }

    /**
     * 获取一个交易所的所有BookTicker
     */
    private List<BookTicker> getBookTickerList(Long orgId, Long exchangeId) throws BizException {
        exchangeIdCheck(exchangeId);
        Set<String> symbols = symbolRepository.getAllSymbolsByExchangeId(exchangeId);
        List<BookTicker> bookTickerList = new ArrayList<>();
        for (String symbol : symbols) {
            try {
                String symbolName = symbolRepository.getSymbolName(orgId, symbol);
                BookTicker bookTicker = getBookTickerCache(exchangeId, symbol);
                bookTicker.setSymbol(symbolName);
                bookTickerList.add(bookTicker);
            } catch (Exception e) {
                log.error("Execute error: ", e);
            }
        }
        return bookTickerList;
    }

    private BookTicker getBookTickerCache(Long exchangeId, String symbol) {
        DepthDTO depthDTO = getDepthSnapshot(exchangeId, symbol);
        return BookTicker.parse(depthDTO);
    }

    /**
     * 获取一个交易所的所有BookTicker
     */
    private List<BookTicker> getOptionBookTickerList(Long orgId, Long exchangeId) throws BizException {
        exchangeIdCheck(exchangeId);
        Set<String> symbols = symbolRepository.getOptionAllSymbolByExchangeId(exchangeId);
        List<BookTicker> bookTickerList = new ArrayList<>();
        for (String symbol : symbols) {
            try {
                String symbolName = symbolRepository.getSymbolName(orgId, symbol);
                BookTicker bookTicker = getBookTickerCache(exchangeId, symbol);
                bookTicker.setSymbol(symbolName);
                bookTickerList.add(bookTicker);
            } catch (Exception e) {
                log.error("Execute error: ", e);
            }
        }
        return bookTickerList;
    }

    private List<BookTicker> getFuturesBookTickerList(Long orgId, Long exchangeId) throws BizException {
        exchangeIdCheck(exchangeId);
        Set<String> symbols = symbolRepository.getFuturesAllSymbolsByExchangeId(exchangeId);
        List<BookTicker> bookTickerList = new ArrayList<>();
        for (String symbol : symbols) {
            try {
                String symbolName = symbolRepository.getSymbolName(orgId, symbol);
                BookTicker bookTicker = getBookTickerCache(exchangeId, symbol);
                bookTicker.setSymbol(symbolName);
                bookTickerList.add(bookTicker);
            } catch (Exception e) {
                log.error("Execute error: ", e);
            }
        }
        return bookTickerList;
    }

    public List<BookTicker> getBookTickerList(String host) throws BizException {
        Long orgId = symbolRepository.getOrgIdByDomain(host);
        Long exchangeId = symbolRepository.getFirstExchangeIdByOrgId(orgId);
        if (Objects.isNull(exchangeId)) {
            return Collections.emptyList();
        }
        return getBookTickerList(orgId, exchangeId);
    }

    public List<BookTicker> getOptionBookTickerList(String host) throws BizException {
        Long orgId = symbolRepository.getOrgIdByDomain(host);
        Long exchangeId = symbolRepository.getFirstExchangeIdByOrgId(orgId);
        if (Objects.isNull(exchangeId)) {
            return Collections.emptyList();
        }
        return getOptionBookTickerList(orgId, exchangeId);
    }

    public List<BookTicker> getFuturesBookTickerList(String host) throws BizException {
        Long orgId = symbolRepository.getOrgIdByDomain(host);
        Long exchangeId = symbolRepository.getFirstExchangeIdByOrgId(orgId);
        if (Objects.isNull(exchangeId)) {
            return Collections.emptyList();
        }
        return getFuturesBookTickerList(orgId, exchangeId);
    }

    /**
     * 获取原始历史成交
     */
    public List<TicketDTO> getTrades(Long exchangeId, String symbol) throws BizException {
        commonCheck(exchangeId, symbol);
        try {
            exchangeId = symbolRepository.getSharedExchangeId(exchangeId, symbol);
            TradeListener tradeListener = (TradeListener) context
                .getBean(BeanUtils.getTradesBeanName(exchangeId, symbol));
            ConcurrentLinkedDeque<TicketDTO> ticketDTOConcurrentLinkedDeque = tradeListener.getSnapshotData();
            return new LinkedList<>(ticketDTOConcurrentLinkedDeque);
        } catch (NoSuchBeanDefinitionException e) {
            return Collections.emptyList();
        }
    }

    public List<TicketDTO> getTrades(String host, String symbol) throws BizException {
        Long orgId = symbolRepository.getOrgIdByDomain(host);
        symbol = symbolRepository.getRealSymbolId(orgId, symbol);
        Long exchangeId = symbolRepository.getExchangeIdBySymbolAndOrgId(symbol, orgId);
        // ticket里没有symbol
        return getTrades(exchangeId, symbol);
    }

    /**
     * 获取历史k线(过滤)
     */
    public List<KlineItemDTO> getHistoryKline(String host, Long exchangeId, String symbol, KlineTypes klineTypes, long from, long to)
        throws ExecutionException {
        Long sharedExchangeId = symbolRepository.getSharedExchangeId(exchangeId, symbol);
        Long orgId = symbolRepository.getOrgIdByDomainNoError(host);
        long filterKlineStartId = FilterHistoryKlineUtil.getInstance().getFilterHistoryKlineTimeStartId(orgId, symbol, klineTypes);
        if (filterKlineStartId > to) {
            return Collections.emptyList();
        } else {
            from = Math.max(filterKlineStartId, from);
        }
        QuoteIndex.KlineIndex klineIndex = QuoteIndex.KlineIndex.builder()
            .exchangeId(sharedExchangeId)
            .symbol(symbol)
            .klineType(klineTypes.getInterval())
            .from(from)
            .to(to)
            .build();
        List<KLine> kLineList = historyKlineCache.get(klineIndex);
        List<KlineItemDTO> klineItemDTOList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(kLineList)) {
            klineItemDTOList = kLineList.stream()
                .map(data -> KlineItemDTO.parse(data, symbol))
                .collect(Collectors.toList());
        }
        return klineItemDTOList;
    }

    public List<KlineItemDTO> getMemIndexKline(String symbol, KlineTypes klineTypes, long from, long to) {
        try {
            IndexKlineListener indexKlineListener = context
                .getBean(BeanUtils.getIndexKlineBeanName(symbol, klineTypes.getInterval()), IndexKlineListener.class);
            ConcurrentLinkedDeque<KlineItemDTO> klineItemDTOS = indexKlineListener.getSnapshotData();
            List<KlineItemDTO> resultList = new ArrayList<>();
            for (KlineItemDTO klineItemDTO : klineItemDTOS) {
                if (klineItemDTO.getTime() > from && klineItemDTO.getTime() <= to) {
                    resultList.add(klineItemDTO);
                }
            }
            return resultList;
        } catch (NoSuchBeanDefinitionException e) {
            return Collections.emptyList();
        }
    }

    /**
     * 从缓存中获取k线(过滤)
     */
    public List<KlineItemDTO> getMemKline(String host, Long exchangeId, String symbol, KlineTypes klineTypes, long from, long to)
        throws BizException {
        commonCheck(exchangeId, symbol);
        try {
            //判断是否要截取
            Long sharedExchangeId = symbolRepository.getSharedExchangeId(exchangeId, symbol);
            Long orgId = symbolRepository.getOrgIdByDomainNoError(host);
            long filterKlineStartId = FilterHistoryKlineUtil.getInstance().getFilterHistoryKlineTimeStartId(orgId, symbol, klineTypes);
            if (filterKlineStartId > to) {
                return Collections.emptyList();
            } else {
                from = Math.max(filterKlineStartId, from);
            }
            KlineListener klineListener = (KlineListener) context
                .getBean(BeanUtils.getKlineBeanName(sharedExchangeId, symbol, klineTypes.getInterval()));
            ConcurrentLinkedDeque<KlineItemDTO> klineItemDTOS = klineListener.getSnapshotData();
            List<KlineItemDTO> resultList = new ArrayList<>();
            for (KlineItemDTO klineItemDTO : klineItemDTOS) {
                if (klineItemDTO.getTime() >= from && klineItemDTO.getTime() <= to) {
                    resultList.add(klineItemDTO);
                }
            }
            return resultList;
        } catch (NoSuchBeanDefinitionException e) {
            return Collections.emptyList();
        }
    }

    /**
     *  获取缓存的k线（已做共享k线的历史排除）
     */
    public List<KLine> getOriginMemKline(Long orgId, Long exchangeId, String symbol, KlineTypes klineTypes, long from, long to)
            throws BizException {
        commonCheck(exchangeId, symbol);
        try {
            Long sharedExchangeId = symbolRepository.getSharedExchangeId(exchangeId, symbol);
            //判断是否要截取
            long filterKlineStartId = FilterHistoryKlineUtil.getInstance().getFilterHistoryKlineTimeStartId(orgId, symbol, klineTypes);
            if (filterKlineStartId > to) {
                return Collections.emptyList();
            } else {
                from = Math.max(filterKlineStartId, from);
            }
            KlineListener klineListener = (KlineListener) context
                .getBean(BeanUtils.getKlineBeanName(sharedExchangeId, symbol, klineTypes.getInterval()));
            ConcurrentLinkedDeque<KLine> kLineList = klineListener.getOriginSnapshotData();
            List<KLine> resultList = new ArrayList<>();
            for (KLine kLine : kLineList) {
                if (kLine.getId() >= from && kLine.getId() <= to) {
                    resultList.add(kLine);
                }
            }
            return resultList;
        } catch (NoSuchBeanDefinitionException e) {
            return Collections.emptyList();
        }
    }

    public List<KlineItemDTO> getLatestMemIndexKline(String symbol, KlineTypes klineTypes, int limit) {
        try {
            IndexKlineListener indexKlineListener = context.getBean(BeanUtils
                    .getIndexKlineBeanName(symbol, klineTypes.getInterval()),
                IndexKlineListener.class);
            ConcurrentLinkedDeque<KlineItemDTO> klineItemDTOS = indexKlineListener.getSnapshotData();
            LinkedList<KlineItemDTO> resultList = new LinkedList<>();
            int size = klineItemDTOS.size();
            if (size <= limit) {
                Iterator<KlineItemDTO> it = klineItemDTOS.iterator();
                for (int i = 0; i < size; ++i) {
                    if (it.hasNext()) {
                        resultList.add(it.next());
                    } else {
                        break;
                    }
                }
            } else {
                Iterator<KlineItemDTO> desIt = klineItemDTOS.descendingIterator();
                for (int i = 0; i < limit; ++i) {
                    if (desIt.hasNext()) {
                        resultList.addFirst(desIt.next());
                    } else {
                        break;
                    }
                }
            }
            return resultList;
        } catch (NoSuchBeanDefinitionException e) {
            return Collections.emptyList();
        }
    }

    /**
     * 获取近期的缓存k线（已处理共享k线）
     */
    public List<KlineItemDTO> getLatestMemKline(String host, Long exchangeId, String symbol, KlineTypes klineTypes, int limit) throws BizException {
        commonCheck(exchangeId, symbol);
        try {
            Long sharedExchangeId = symbolRepository.getSharedExchangeId(exchangeId, symbol);
            //判断是否要截取
            Long orgId = symbolRepository.getOrgIdByDomainNoError(host);
            long filterKlineStartId = FilterHistoryKlineUtil.getInstance().getFilterHistoryKlineTimeStartId(orgId, symbol, klineTypes);
            if (filterKlineStartId > System.currentTimeMillis()) {
                return Collections.emptyList();
            }
            KlineListener klineListener = (KlineListener) context
                .getBean(BeanUtils.getKlineBeanName(sharedExchangeId, symbol, klineTypes.getInterval()));
            ConcurrentLinkedDeque<KlineItemDTO> klineItemDTOS = klineListener.getSnapshotData();
            LinkedList<KlineItemDTO> resultList = new LinkedList<>();
            int size = klineItemDTOS.size();
            if (size <= limit) {
                Iterator<KlineItemDTO> it = klineItemDTOS.iterator();
                for (int i = 0; i < size; ++i) {
                    if (it.hasNext()) {
                        KlineItemDTO klineItemDTO = it.next();
                        if(klineItemDTO.getCurId() >= filterKlineStartId){
                            resultList.add(klineItemDTO);
                        }
                    } else {
                        break;
                    }
                }
            } else {
                Iterator<KlineItemDTO> desIt = klineItemDTOS.descendingIterator();
                for (int i = 0; i < limit; ++i) {
                    if (desIt.hasNext()) {
                        KlineItemDTO klineItemDTO = desIt.next();
                        if(klineItemDTO.getCurId() >= filterKlineStartId){
                            resultList.addFirst(klineItemDTO);
                        }
                    } else {
                        break;
                    }
                }
            }
            return resultList;
        } catch (NoSuchBeanDefinitionException e) {
            return Collections.emptyList();
        }
    }

    /**
     * 获取原始k线数据(已做共享历史处理)
     */
    public List<KlineItemDTO> getMemKline(String host, String symbol, KlineTypes klineTypes, long from, long to)
        throws BizException {
        Long orgId = symbolRepository.getOrgIdByDomain(host);
        Long exchangeId = symbolRepository.getExchangeIdBySymbolAndOrgId(symbol, orgId);
        return getMemKline(host, exchangeId, symbol, klineTypes, from, to);
    }

    /**
     *  从缓存的k线中查询（已做历史k线的排除）
     */
    public List<KLine> getOriginMemKline(String host, String symbol, KlineTypes klineTypes, long from, long to)
        throws BizException {
        Long orgId = symbolRepository.getOrgIdByDomain(host);
        String paramSymbol = symbol;
        symbol = symbolRepository.getRealSymbolId(orgId, symbol);
        Long exchangeId = symbolRepository.getExchangeIdBySymbolAndOrgId(symbol, orgId);
        List<KLine> kLineList = getOriginMemKline(orgId, exchangeId, symbol, klineTypes, from, to);
        if (!StringUtils.equals(paramSymbol, symbol)) {
            List<KLine> newSymbolKlineList = new ArrayList<>();
            for (KLine kLine : kLineList) {
                newSymbolKlineList.add(kLine
                    .toBuilder()
                    .setSymbol(paramSymbol)
                    .build());
            }
            kLineList = newSymbolKlineList;
        }
        return kLineList;
    }

    public List<KlineItemDTO> getLatestMemKline(String host, String symbol, KlineTypes klineTypes, int limit)
        throws BizException {
        Long orgId = symbolRepository.getOrgIdByDomain(host);
        Long exchangeId = symbolRepository.getExchangeIdBySymbolAndOrgId(symbol, orgId);
        return getLatestMemKline(host, exchangeId, symbol, klineTypes, limit);
    }

    private void exchangeIdCheck(Long exchangeId) throws BizException {
        if (!symbolRepository.getExchangeIdSet().contains(exchangeId)) {
            throw new BizException(ErrorCodeEnum.EXCHANGE_ID_ERROR);
        }
    }

    private void symbolCheck(String symbol) throws BizException {
        if (!symbolRepository.isSupportSymbol(symbol)) {
            throw new BizException(ErrorCodeEnum.SYMBOLS_NOT_SUPPORT);
        }
    }

    private void commonCheck(Long exchangeId, String symbol) throws BizException {
        exchangeIdCheck(exchangeId);
        symbolCheck(symbol);
    }
}
