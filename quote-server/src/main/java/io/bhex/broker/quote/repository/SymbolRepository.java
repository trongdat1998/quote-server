package io.bhex.broker.quote.repository;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.bhex.base.token.SymbolDetail;
import io.bhex.base.token.TokenCategory;
import io.bhex.broker.grpc.basic.Symbol;
import io.bhex.broker.grpc.broker.Broker;
import io.bhex.broker.quote.config.ConfigCenter;
import io.bhex.broker.quote.data.QuoteIndex;
import io.bhex.broker.quote.data.SymbolUpdateInfo;
import io.bhex.broker.quote.enums.ErrorCodeEnum;
import io.bhex.broker.quote.exception.BizException;
import io.bhex.broker.quote.index.SymbolIndex;
import io.bhex.broker.quote.util.KeyUtils;
import io.netty.util.internal.StringUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

import static io.bhex.base.token.TokenCategory.FUTURE_CATEGORY;
import static io.bhex.base.token.TokenCategory.MAIN_CATEGORY;
import static io.bhex.base.token.TokenCategory.OPTION_CATEGORY;
import static io.bhex.broker.quote.common.SymbolConstants.DOT;
import static io.bhex.broker.quote.util.ConstantUtil.DEFAULT_MERGE_SCALE;

/**
 * Store symbol and exchangeId info
 */
@Slf4j
@Repository
@Data
public class SymbolRepository {

    private final BhServerRepository bhServerRepository;

    private final ConfigCenter configCenter;

    /**
     * 按交易所把symbol分组
     */
    private Map<Long, Set<String>> allExchangeSymbolsMap = new HashMap<>();
    private Map<Long, Set<String>> otherExchangeIdSymbolsMap = new HashMap<>();
    private Map<Long, Set<String>> exchangeIdSymbolsMap = new HashMap<>();
    private Map<Long, Set<String>> optionExchangeIdSymbolsMap = new HashMap<>();
    private Map<Long, Set<String>> futuresExchangeIdSymbolsMap = new HashMap<>();

    /**
     * orgId exchangeIdList
     */
    private Map<Long, Set<Long>> orgIdExchangeIdSetMap = new HashMap<>();
    private Set<Broker> brokerSet = new HashSet<>();
    /**
     * orgId -> broker
     */
    private ConcurrentMap<Long, Broker> supportedBrokerMap = new ConcurrentHashMap<>();

    // orgId -> quoteIndex -> symbol
    private ConcurrentHashMap<Long, ConcurrentMap<QuoteIndex, Symbol>> brokerSymbolSetMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Long, ConcurrentMap<QuoteIndex, Symbol>> futuresBrokerSymbolSetMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Long, ConcurrentMap<QuoteIndex, Symbol>> optionBrokerSymbolSetMap = new ConcurrentHashMap<>();

    /**
     * 币对对应的券商id
     */
    private ConcurrentMap<QuoteIndex, CopyOnWriteArraySet<Long>> exchangeIdSymbolOrgIdSetMap = new ConcurrentHashMap<>();

    /**
     * orgId -> exchangeId -> symbolId -> symbol
     */
    private ConcurrentMap<Long, ConcurrentMap<QuoteIndex, Symbol>> brokerExchangeSymbolMap = new ConcurrentHashMap<>();
    /**
     * orgId -> symbolId -> Symbol
     */
    private ConcurrentMap<Long, ConcurrentMap<String, Symbol>> orgSymbolMap = new ConcurrentHashMap<>();

    /**
     * 按symbol做org索引
     * symbolId -> orgId -> exchangeIdSet
     * 一个orgId下可能会有多个exchangeId
     */
    private ConcurrentMap<String, SymbolIndex> symbolIndicesMap = new ConcurrentHashMap<>();

    /**
     * org_id_symbol_name -> symbolId
     */
    private ConcurrentMap<String, String> symbolNameOrgSymbolIdMap = new ConcurrentHashMap<>();
    /**
     * org_id_symbol_id -> symbol_name
     */
    private ConcurrentMap<String, String> symbolIdOrgSymbolNameMap = new ConcurrentHashMap<>();

    /**
     * 所有支持的broker  symbol的type
     */
    private Map<String, Symbol> brokerSymbolMap = new HashMap<>();

    /**
     * 交易所中symbol的合并深度配置
     */
    private Map<String, int[]> symbolsMergedListMap = new HashMap<>();

    /**
     * 所有的exchangeId
     */
    private Set<Long> exchangeIdSet = new CopyOnWriteArraySet<>();

    /**
     * exchangeId -> string -> shardExchangeId
     */
    private ConcurrentMap<Long, ConcurrentMap<String, Long>> sharedExchangeMap = new ConcurrentHashMap<>();
    /**
     * 反向的共享关系
     */
    private ConcurrentMap<Long, ConcurrentMap<String, CopyOnWriteArraySet<Long>>> reversedSharedExchangeMap
        = new ConcurrentHashMap<>();

    @Autowired
    public SymbolRepository(ConfigCenter configCenter, BhServerRepository bhServerRepository) {
        this.configCenter = configCenter;
        this.bhServerRepository = bhServerRepository;
    }

    public Symbol getSymbol(String symbol) {
        return this.brokerSymbolMap.get(symbol);
    }

    public String getSymbolNameByBrokerSymbol(String symbolId) {
        Symbol symbol = getSymbol(symbolId);
        if (symbol == null) {
            return symbolId;
        }
        return symbol.getSymbolName();
    }

    public boolean isSupportSymbol(String symbolId) {
        return this.brokerSymbolMap.containsKey(symbolId);
    }

    public int getSymbolType(String symbol) {
        Symbol symbolObj = getSymbol(symbol);
        if (Objects.isNull(symbolObj)) {
            return TokenCategory.UNKNOWN_TOKEN_CATEGORY_VALUE;
        }
        return symbolObj.getCategory();
    }

    public String getRealSymbolId(Long orgId, String symbol) {
        String key = KeyUtils.getSymbolNameKey(orgId, symbol);
        if (!symbolNameOrgSymbolIdMap.containsKey(key)) {
            return symbol;
        }
        return symbolNameOrgSymbolIdMap.get(key);
    }

    public String getSymbolName(Long orgId, String symbolId) {
        String key = KeyUtils.getSymbolIdKey(orgId, symbolId);
        if (!symbolIdOrgSymbolNameMap.containsKey(key)) {
            return symbolId;
        }
        return symbolIdOrgSymbolNameMap.get(key);
    }

    public Set<Long> getOrgIdsByExchangeIdAndSymbol(Long exchangeId, String symbol) {
        QuoteIndex quoteIndex = QuoteIndex.builder()
            .exchangeId(exchangeId)
            .symbol(symbol)
            .build();
        CopyOnWriteArraySet<Long> orgIdSets = exchangeIdSymbolOrgIdSetMap.get(quoteIndex);
        if (Objects.isNull(orgIdSets)) {
            return new CopyOnWriteArraySet<>();
        }
        return orgIdSets;
    }

    /**
     * 获取币对状态是否可显示
     */
    public boolean isOpenSymbol(Long orgId, String symbol) {
        Map<String, Symbol> symbolMap = this.orgSymbolMap.get(orgId);
        if (Objects.isNull(symbolMap)) {
            return false;
        }

        Symbol symbolObj = symbolMap.get(symbol);
        if (Objects.isNull(symbolObj)) {
            return false;
        }
        return symbolObj.getShowStatus();
    }

    /**
     * 获取topN是否展示(不显示屏蔽,topN过滤也屏蔽)
     */
    public boolean isTopNOpenSymbol(Long orgId, String symbol) {
        Map<String, Symbol> symbolMap = this.orgSymbolMap.get(orgId);
        if (Objects.isNull(symbolMap)) {
            return false;
        }

        Symbol symbolObj = symbolMap.get(symbol);
        if (Objects.isNull(symbolObj)) {
            return false;
        }
        return symbolObj.getShowStatus() && !symbolObj.getFilterTopStatus();
    }

    /**
     * 建立共享相关的字典
     */
    public void updateSharedExchangeInfo(Map<Long, List<SymbolDetail>> symbolDetailListMap) {
        if (!symbolDetailListMap.isEmpty()) {
            // 根据当前交易所币对，获取它所共享的交易所Id
            // eg: 604:BTCUSDT->301
            // 用于订阅时
            ConcurrentMap<Long, ConcurrentMap<String, Long>> sharedExchangeMap = new ConcurrentHashMap<>();
            // 根据被共享的交易所，获取所有共享这个币对的交易所列表
            // eg: 301:BTCUSDT->[604,605]
            // 用于发送数据时
            ConcurrentMap<Long, ConcurrentMap<String, CopyOnWriteArraySet<Long>>> reversedSharedExchangeMap
                = new ConcurrentHashMap<>();
            for (Map.Entry<Long, List<SymbolDetail>> longListEntry : symbolDetailListMap.entrySet()) {
                Long exchangeId = longListEntry.getKey();
                List<SymbolDetail> symbols = longListEntry.getValue();
                ConcurrentMap<String, Long> exchangeMap
                    = sharedExchangeMap.computeIfAbsent(exchangeId, v -> new ConcurrentHashMap<>());
                for (SymbolDetail symbol : symbols) {
                    ConcurrentMap<String, CopyOnWriteArraySet<Long>> reversedExchangeMap = reversedSharedExchangeMap
                        .computeIfAbsent(symbol.getMatchExchangeId(), v -> new ConcurrentHashMap<>());
                    exchangeMap.put(symbol.getSymbolId(), symbol.getMatchExchangeId());
                    reversedExchangeMap.computeIfAbsent(symbol.getSymbolId(), s -> new CopyOnWriteArraySet<>());
                    reversedExchangeMap.computeIfPresent(symbol.getSymbolId(), (s, set) -> {
                        set.add(exchangeId);
                        return set;
                    });
                }
            }
            this.sharedExchangeMap = sharedExchangeMap;
            this.reversedSharedExchangeMap = reversedSharedExchangeMap;
        }
    }

    public Set<Long> getReversedSharedExchangeIds(Long exchangeId, String symbol) {
        return this.reversedSharedExchangeMap.getOrDefault(exchangeId, new ConcurrentHashMap<>())
            .getOrDefault(symbol, new CopyOnWriteArraySet<>());
    }

    /**
     * 获取共享交易所ID，如果没有就返回原来交易所ID
     */
    public Long getSharedExchangeId(Long sourceExchangeId, String symbol) {
        Map<String, Long> symbolSharedExchangeIdMap = this.sharedExchangeMap.get(sourceExchangeId);
        if (Objects.isNull(symbolSharedExchangeIdMap)) {
            return sourceExchangeId;
        }
        Long sharedExchangeId = symbolSharedExchangeIdMap.get(symbol);
        if (Objects.isNull(sharedExchangeId)) {
            return sourceExchangeId;
        }
        return sharedExchangeId;
    }

    /**
     * 1. 更新exchangeId symbol 对应的orgId集合，当此币对有行情更新时发送数据到每个所属的orgId
     * 2. 更新总的数据，orgId -> exchangeId+symbol -> Symbol, orgId对应broker下所有的币对字典
     * 3. 更新orgId -> symbolId -> Symbol, orgId对应broker下的symbolId对应的币对字典
     */
    public void updateBrokerSymbols(ConcurrentMap<Long, ConcurrentMap<QuoteIndex, Symbol>> brokerSymbolMap,
                                    ConcurrentMap<Long, Broker> brokerMap) {
        ConcurrentMap<QuoteIndex, CopyOnWriteArraySet<Long>> exchangeIdSymbolOrgIdSetMap
            = new ConcurrentHashMap<>();

        ConcurrentMap<Long, ConcurrentMap<QuoteIndex, Symbol>> brokerExchangeSymbolMap
            = new ConcurrentHashMap<>();

        ConcurrentMap<Long, ConcurrentMap<String, Symbol>> orgSymbolMap = new ConcurrentHashMap<>();

        ConcurrentMap<String, String> symbolNameOrgSymbolIdMap = new ConcurrentHashMap<>();
        ConcurrentMap<String, String> symbolIdOrgSymbolNameMap = new ConcurrentHashMap<>();

        for (Map.Entry<Long, ConcurrentMap<QuoteIndex, Symbol>> brokerMapEntry : brokerSymbolMap.entrySet()) {
            Long brokerId = brokerMapEntry.getKey();
            Broker broker = brokerMap.get(brokerId);

            Map<QuoteIndex, Symbol> quoteIndexSymbolMap = brokerMapEntry.getValue();

            ConcurrentMap<QuoteIndex, Symbol> exchangeSymbolMap = brokerExchangeSymbolMap.get(broker.getOrgId());
            if (Objects.isNull(exchangeSymbolMap)) {
                exchangeSymbolMap = new ConcurrentHashMap<>();
                brokerExchangeSymbolMap.put(broker.getOrgId(), exchangeSymbolMap);
            }

            ConcurrentMap<String, Symbol> subOrgSymbolMap = orgSymbolMap.get(broker.getOrgId());
            if (Objects.isNull(subOrgSymbolMap)) {
                subOrgSymbolMap = new ConcurrentHashMap<>();
                orgSymbolMap.put(broker.getOrgId(), subOrgSymbolMap);
            }

            for (Map.Entry<QuoteIndex, Symbol> quoteIndexSymbolEntry : quoteIndexSymbolMap.entrySet()) {
                Symbol symbol = quoteIndexSymbolEntry.getValue();
                QuoteIndex quoteIndex = quoteIndexSymbolEntry.getKey();
                String symbolNameKey = KeyUtils.getSymbolNameKey(broker.getOrgId(), symbol.getSymbolName());
                symbolNameOrgSymbolIdMap.put(symbolNameKey, symbol.getSymbolId());
                String symbolIdKey = KeyUtils.getSymbolIdKey(broker.getOrgId(), symbol.getSymbolId());
                symbolIdOrgSymbolNameMap.put(symbolIdKey, symbol.getSymbolName());

                subOrgSymbolMap.put(symbol.getSymbolId(), symbol);

                exchangeSymbolMap.put(quoteIndex, symbol);
                CopyOnWriteArraySet<Long> orgIdSet = exchangeIdSymbolOrgIdSetMap.get(quoteIndex);
                if (Objects.isNull(orgIdSet)) {
                    orgIdSet = new CopyOnWriteArraySet<>();
                    exchangeIdSymbolOrgIdSetMap.put(quoteIndex, orgIdSet);
                }
                orgIdSet.add(symbol.getOrgId());

            }
        }

        this.symbolIdOrgSymbolNameMap = symbolIdOrgSymbolNameMap;
        this.symbolNameOrgSymbolIdMap = symbolNameOrgSymbolIdMap;
        this.exchangeIdSymbolOrgIdSetMap = exchangeIdSymbolOrgIdSetMap;
        this.brokerExchangeSymbolMap = brokerExchangeSymbolMap;
        this.orgSymbolMap = orgSymbolMap;
    }

    /**
     * 1. 对币对按业务进行分类
     * 2. 建立symbol字典, 所有的symbolId的配置都一样
     *
     * @return Different symbolList
     */
    public SymbolUpdateInfo updateSymbols(List<Symbol> symbolList) {


        SymbolUpdateInfo.SymbolUpdateInfoBuilder symbolUpdateInfoBuilder = SymbolUpdateInfo.builder();
        Map<Long, Set<String>> otherExchangeIdSymbolsMap = new HashMap<>();
        Map<Long, Set<String>> exchangeIdSymbolsMap = new HashMap<>();
        Map<Long, Set<String>> optionExchangeIdSymbolsMap = new HashMap<>();
        Map<Long, Set<String>> futuresExchangeIdSymbolsMap = new HashMap<>();
        Map<Long, Set<String>> allExchangeSymbolsMap = new HashMap<>();

        Map<String, Symbol> brokerSymbolMap = new HashMap<>();
        ConcurrentMap<String, SymbolIndex> symbolIndicesMap = new ConcurrentHashMap<>();
        Map<Long, Set<Long>> orgIdExchangeIdSetMap = new HashMap<>();
        Map<String, int[]> symbolsMergedListMap = new HashMap<>();

        Set<Symbol> diffSymbolSet = new HashSet<>();
        Map<Long, Set<String>> pendingRemovedSymbolMap = new HashMap<>();

        List<Symbol> supportSymbolList = new ArrayList<>();
        for (Symbol symbol : symbolList) {
            brokerSymbolMap.put(symbol.getSymbolId(), symbol);
            //if (filterSymbol(symbol)) {
            //    continue;
            //}
            supportSymbolList.add(symbol);
        }

        for (Symbol symbol : supportSymbolList) {


            Set<String> existExchangeIdSymbols = this.allExchangeSymbolsMap.get(symbol.getExchangeId());
            if ((existExchangeIdSymbols == null || !existExchangeIdSymbols.contains(symbol.getSymbolId()))) {
                diffSymbolSet.add(symbol);
            }

            SymbolIndex symbolIndex = symbolIndicesMap.get(symbol.getSymbolId());
            if (Objects.isNull(symbolIndex)) {
                symbolIndex = SymbolIndex.builder()
                    .symbol(symbol.getSymbolId())
                    .orgIdExchangeIdMap(Maps.newConcurrentMap())
                    .build();
                symbolIndicesMap.put(symbol.getSymbolId(), symbolIndex);
            }
            symbolIndex.addExchangeId(symbol.getExchangeId(), symbol.getOrgId());

            if (symbol.getCategory() == MAIN_CATEGORY.getNumber()) {
                Set<String> exchangeIdSymbols =
                    exchangeIdSymbolsMap.computeIfAbsent(symbol.getExchangeId(), k -> new HashSet<>());
                exchangeIdSymbols.add(symbol.getSymbolId());
            } else if (symbol.getCategory() == OPTION_CATEGORY.getNumber()) {
                // 期权
                Set<String> exchangeIdSymbols =
                    optionExchangeIdSymbolsMap.computeIfAbsent(symbol.getExchangeId(), k -> new HashSet<>());
                exchangeIdSymbols.add(symbol.getSymbolId());
            } else if (symbol.getCategory() == FUTURE_CATEGORY.getNumber()) {
                Set<String> exchangeIdSymbols =
                    futuresExchangeIdSymbolsMap.computeIfAbsent(symbol.getExchangeId(), k -> new HashSet<>());
                exchangeIdSymbols.add(symbol.getSymbolId());
            } else {
                Set<String> exchangeIdSymbols =
                    otherExchangeIdSymbolsMap.computeIfAbsent(symbol.getExchangeId(), k -> new HashSet<>());
                exchangeIdSymbols.add(symbol.getSymbolId());
            }
            Set<String> exchangeIdSymbols = allExchangeSymbolsMap.computeIfAbsent(symbol.getExchangeId(),
                k -> new HashSet<>());
            exchangeIdSymbols.add(symbol.getSymbolId());

            // update orgId
            Set<Long> orgExchangeIdSet = orgIdExchangeIdSetMap.get(symbol.getOrgId());
            if (Objects.isNull(orgExchangeIdSet)) {
                orgExchangeIdSet = new HashSet<>();
                orgIdExchangeIdSetMap.put(symbol.getOrgId(), orgExchangeIdSet);
            }
            orgExchangeIdSet.add(symbol.getExchangeId());

            // 去重digit merge
            String[] mergedList = StringUtils.split(StringUtils.trim(symbol.getDigitMerge()), StringUtil.COMMA);
            List<String> mergedStringList = new ArrayList<>();
            for (String s : mergedList) {
                if (!mergedStringList.contains(s)) {
                    mergedStringList.add(s);
                }
            }

            int[] mergedArray = new int[mergedStringList.size() + 1];
            for (int i = 0; i < mergedStringList.size(); i++) {
                String merged = StringUtils.trim(mergedStringList.get(i));
                mergedArray[i] = getScaleFromNumber(merged);
            }

            mergedArray[mergedArray.length - 1] = DEFAULT_MERGE_SCALE;
            if (!symbolsMergedListMap.containsKey(symbol.getSymbolId())) {
                symbolsMergedListMap.put(symbol.getSymbolId(), mergedArray);
            }
        }

        // 过滤出要删除的SYMBOL
        for (Map.Entry<Long, Set<String>> longSetEntry : this.allExchangeSymbolsMap.entrySet()) {
            Long exchangeId = longSetEntry.getKey();
            Set<String> symbolSet = longSetEntry.getValue();
            for (String symbol : symbolSet) {
                boolean contain = false;
                for (Symbol bhSymbol : supportSymbolList) {
                    if (bhSymbol.getExchangeId() == exchangeId && symbol.equalsIgnoreCase(bhSymbol.getSymbolId())) {
                        contain = true;
                        break;
                    }
                }

                if (!contain) {
                    Set<String> pendingRemovedSymbolSets = pendingRemovedSymbolMap
                        .computeIfAbsent(exchangeId, k -> new HashSet<>());
                    pendingRemovedSymbolSets.add(symbol);
                }
            }
        }

        this.allExchangeSymbolsMap = allExchangeSymbolsMap;
        this.otherExchangeIdSymbolsMap = otherExchangeIdSymbolsMap;
        this.exchangeIdSymbolsMap = exchangeIdSymbolsMap;
        this.optionExchangeIdSymbolsMap = optionExchangeIdSymbolsMap;
        this.futuresExchangeIdSymbolsMap = futuresExchangeIdSymbolsMap;
        this.symbolIndicesMap = symbolIndicesMap;
        this.orgIdExchangeIdSetMap = orgIdExchangeIdSetMap;
        this.symbolsMergedListMap = symbolsMergedListMap;
        this.brokerSymbolMap = brokerSymbolMap;

        return symbolUpdateInfoBuilder
            .newSymbolSet(diffSymbolSet)
            .pendingRemovedSymbolMap(pendingRemovedSymbolMap)
            .build();
    }

    public Map<Long, Set<String>> getAllExchangeIdSymbolMap() {
        Map<Long, Set<String>> exchangeIdSymbolMap = new HashMap<>();
        this.optionExchangeIdSymbolsMap.forEach((k, v) -> {
            Set<String> sets = exchangeIdSymbolMap.computeIfAbsent(k, e -> new HashSet<>());
            sets.addAll(v);
        });
        this.exchangeIdSymbolsMap.forEach((k, v) -> {
            Set<String> sets = exchangeIdSymbolMap.computeIfAbsent(k, e -> new HashSet<>());
            sets.addAll(v);
        });
        this.futuresExchangeIdSymbolsMap.forEach((k, v) -> {
            Set<String> sets = exchangeIdSymbolMap.computeIfAbsent(k, e -> new HashSet<>());
            sets.addAll(v);
        });
        return exchangeIdSymbolMap;
    }

    public void update(List<Broker> brokersList) {
        this.brokerSet = new HashSet<>(brokersList);

        ConcurrentMap<Long, Broker> brokerConcurrentMap = new ConcurrentHashMap<>();
        for (Broker broker : brokersList) {
            brokerConcurrentMap.put(broker.getOrgId(), broker);
        }
        this.supportedBrokerMap = brokerConcurrentMap;
    }

    public Long getOrgIdByDomain(String host) throws BizException {
        if (StringUtils.isEmpty(host)) {
            throw new BizException(ErrorCodeEnum.BROKER_NOT_SUPPORT);
        }
        for (Broker broker : brokerSet) {
            for (String domain : broker.getApiDomainsList()) {
                if (host.contains(domain)) {
                    return broker.getOrgId();
                }
            }
        }
        throw new BizException(ErrorCodeEnum.BROKER_NOT_SUPPORT);
    }

    /**
     * 用于不报错的orgId获取, 避免影响原有业务
     *
     * @param host
     * @return
     */
    public Long getOrgIdByDomainNoError(String host) {
        for (Broker broker : brokerSet) {
            for (String domain : broker.getApiDomainsList()) {
                if (host.contains(domain)) {
                    return broker.getOrgId();
                }
            }
        }
        log.warn("no orgId by domain!{}", host);
        return 0L;
    }

    public Long getExchangeIdBySymbolAndOrgId(String symbol, Long orgId) throws BizException {
        SymbolIndex symbolIndices = this.symbolIndicesMap.get(symbol);
        if (Objects.isNull(symbolIndices)) {
            throw new BizException(ErrorCodeEnum.SYMBOLS_ERROR);
        }
        Long exchangeId = symbolIndices.getExchangeId(orgId);
        if (Objects.isNull(exchangeId)) {
            throw new BizException(ErrorCodeEnum.SYMBOLS_NOT_SUPPORT);
        }
        return exchangeId;
    }

    public int getMaxScale(String symbol) {
        int[] mergedArray = getMergedArray(symbol);
        if (mergedArray.length == 0) {
            throw new IllegalStateException("symbol " + symbol + " merged array is empty");
        }

        if (mergedArray.length == 1) {
            return mergedArray[0];
        }

        int max = mergedArray[0];
        for (int i = 0; i < mergedArray.length - 1; i++) {
            if (mergedArray[i] > max) {
                max = mergedArray[i];
            }
        }
        return max;

    }

    public int[] getMergedArray(String symbol) {
        if (this.symbolsMergedListMap.containsKey(symbol)) {
            return this.symbolsMergedListMap.get(symbol);
        }
        return new int[0];
    }

    public static int getScaleFromNumber(String mergeScale) {
        if (StringUtils.isEmpty(mergeScale)) {
            return DEFAULT_MERGE_SCALE;
        }
        if (StringUtils.containsIgnoreCase(mergeScale, DOT)) {
            int commaIndex = mergeScale.lastIndexOf(DOT);
            int len = mergeScale.length();
            return len - commaIndex - 1;
        }
        return -mergeScale.length();
    }

    public Long getFirstExchangeIdByOrgId(Long orgId) throws BizException {
        Set<Long> exchangeIdSet = this.orgIdExchangeIdSetMap.get(orgId);
        if (CollectionUtils.isEmpty(exchangeIdSet)) {
            throw new BizException(ErrorCodeEnum.NO_BUSINESS_WITH_EXCHANGE);
        }
        return exchangeIdSet.iterator().next();
    }

    public Set<String> getAllSymbolsByExchangeId(Long exchangeId) {
        Set<String> sets = exchangeIdSymbolsMap.get(exchangeId);
        if (sets == null) {
            return new HashSet<>();
        }
        return sets;
    }

    public Set<String> getOptionAllSymbolByExchangeId(Long exchangeId) {
        Set<String> sets = optionExchangeIdSymbolsMap.get(exchangeId);
        if (Objects.isNull(sets)) {
            return Sets.newHashSet();
        }
        return sets;
    }

    public Set<String> getFuturesAllSymbolsByExchangeId(Long exchangeId) {
        Set<String> sets = futuresExchangeIdSymbolsMap.get(exchangeId);
        if (Objects.isNull(sets)) {
            return Sets.newHashSet();
        }
        return sets;
    }

    /**
     * 是否过滤掉此broker
     * if true表示不支持此broker, false则为支持
     */
    public boolean filterBroker(Broker broker) {
        if (CollectionUtils.isNotEmpty(configCenter.getInServiceBrokers())) {
            return !configCenter.getInServiceBrokers().contains(broker.getOrgId());
        }

        if (CollectionUtils.isNotEmpty(configCenter.getOutOfServiceBrokers())) {
            return configCenter.getOutOfServiceBrokers().contains(broker.getOrgId());
        }
        return false;
    }
}
