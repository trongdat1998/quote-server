package io.bhex.broker.quote.job;

import io.bhex.base.token.SymbolDetail;
import io.bhex.base.token.TokenCategory;
import io.bhex.broker.grpc.basic.Symbol;
import io.bhex.broker.grpc.broker.Broker;
import io.bhex.broker.quote.client.QuoteEvent;
import io.bhex.broker.quote.client.QuoteEventTypeEnum;
import io.bhex.broker.quote.client.QuoteInitializeManager;
import io.bhex.broker.quote.data.BrokerSymbolUpdateInfo;
import io.bhex.broker.quote.data.QuoteIndex;
import io.bhex.broker.quote.enums.OrderConstants;
import io.bhex.broker.quote.enums.RealtimeIntervalEnum;
import io.bhex.broker.quote.enums.TopicEnum;
import io.bhex.broker.quote.listener.BrokerListener;
import io.bhex.broker.quote.listener.SlowBrokerListener;
import io.bhex.broker.quote.listener.TopNListener;
import io.bhex.broker.quote.repository.BhServerRepository;
import io.bhex.broker.quote.repository.BrokerRepository;
import io.bhex.broker.quote.repository.SymbolRepository;
import io.bhex.broker.quote.util.BeanUtils;
import io.bhex.broker.quote.util.BrokerAuthUtil;
import io.bhex.broker.quote.util.FilterHistoryKlineUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static io.bhex.broker.quote.listener.TopNListener.ALL;

@Component
@Slf4j
@Order(OrderConstants.ORDER_BROKER_REPOSITORY)
public class BrokerInfoTask {

    private final int TRY_TIME = 5;
    private final long SLEEP_INTERVAL = 5000L;
    private final BrokerRepository brokerRepository;
    private final ApplicationContext context;
    private final SymbolRepository symbolRepository;
    private final QuoteInitializeManager quoteManager;
    private final ScheduledExecutorService scheduledExecutorService;
    @Autowired
    private BhServerRepository bhServerRepository;

    @Value("${bhex.proxy}")
    private Boolean proxy;

    public static final int[] SYMBOL_TYPE_ARRAY = new int[]{
        TokenCategory.OPTION_CATEGORY_VALUE,
        TokenCategory.MAIN_CATEGORY_VALUE,
        TokenCategory.FUTURE_CATEGORY_VALUE,
        ALL};

    @Autowired
    public BrokerInfoTask(BrokerRepository brokerRepository,
                          ApplicationContext context,
                          SymbolRepository symbolRepository,
                          QuoteInitializeManager quoteManager,
                          @Qualifier("scheduledTaskExecutor") ScheduledExecutorService scheduledExecutorService) {
        this.brokerRepository = brokerRepository;
        this.context = context;
        this.symbolRepository = symbolRepository;
        this.quoteManager = quoteManager;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    public void init() {
        log.info("Init symbol started");
        initSymbol();
        log.info("Init symbol finished");
    }

    /**
     * 不断更新symbols => exchangeId和symbol
     */

    @Scheduled(initialDelay = 60_000L, fixedDelay = 30_000L)
    public void fetchSymbols() {
        try {
            // 获取币对信息
            initSymbol();
            refreshSubscription();
        } catch (Exception e) {
            log.error("Fetch symbols ex: ", e);
        }
    }

    private void refreshSubscription() {
        // 根据当前已经有信息，刷新订阅相关操作
        quoteManager.addQuoteEvent(QuoteEvent
            .builder()
            .type(QuoteEventTypeEnum.REFRESH_SUBSCRIPTION)
            .build());
    }

    /**
     * 1. 共享币对发生变化
     * 2. 上下币对
     * 3. 上下broker
     */
    private void initSymbol() {
        int count = 0;
        while (!Thread.currentThread().isInterrupted()) {
            try {

                // 获取broker信息
                List<Broker> brokerList = brokerRepository.getBrokers();
                List<Broker> supportBrokerList = new ArrayList<>();
                // 根据配置的orgId过滤支持的broker列表
                for (Broker broker : brokerList) {
                    if (symbolRepository.filterBroker(broker)) {
                        continue;
                    }
                    supportBrokerList.add(broker);
                }
                // 记录broker list
                symbolRepository.update(supportBrokerList);
                log.info("Get all brokers size [{}] support brokers size [{}]",
                    brokerList.size(), supportBrokerList.size());
                // orgId
                ConcurrentMap<Long, ConcurrentMap<QuoteIndex, Symbol>> brokerSymbolListMap = new ConcurrentHashMap<>();
                // orgId
                ConcurrentMap<Long, Broker> orgBrokerMap = new ConcurrentHashMap<>();
                // exchangeId集合
                Set<Long> exchangeIdSet = new CopyOnWriteArraySet<>();
                ConcurrentMap<Broker, Set<Symbol>> brokerSymbolMap = new ConcurrentHashMap<>();
                // symbol总列表
                List<Symbol> totalSymbolList = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(supportBrokerList)) {
                    // 获取每个broker的币对列表
                    for (Broker broker : supportBrokerList) {
                        Set<Symbol> symbolList = new HashSet<>(brokerRepository.getSymbolListByOrgId(broker.getOrgId()));
                        brokerSymbolMap.put(broker, symbolList);
                        ConcurrentMap<QuoteIndex, Symbol> quoteIndexSymbolMap = new ConcurrentHashMap<>();
                        orgBrokerMap.put(broker.getOrgId(), broker);
                        for (Symbol symbol : symbolList) {
                            totalSymbolList.add(symbol);
                            QuoteIndex quoteIndex = QuoteIndex.builder()
                                .exchangeId(symbol.getExchangeId())
                                .symbol(symbol.getSymbolId())
                                .build();
                            quoteIndexSymbolMap.put(quoteIndex, symbol);

                            // 统计exchangeId集合，用于获取共享信息
                            exchangeIdSet.add(symbol.getExchangeId());
                            // 获取币对是否过滤历史k线
                            FilterHistoryKlineUtil.getInstance().updateFilterHistoryKlineTime(broker.getOrgId(), symbol.getSymbolId(), symbol.getFilterTime());
                        }
                        brokerSymbolListMap.put(broker.getOrgId(), quoteIndexSymbolMap);
                    }
                }

                // 缓存exchangeIdSet
                symbolRepository.setExchangeIdSet(exchangeIdSet);

                // 获取共享信息
                if (CollectionUtils.isNotEmpty(exchangeIdSet)) {
                    Map<Long, List<SymbolDetail>> symbolDetailListMap = new HashMap<>();
                    if (proxy) {
                        //TODO 非gateway模式时，获取的orgId是0，后期平台orgId生效时再使用所有的orgId合并结果处理
                        Long orgId = BrokerAuthUtil.getOrgIdSet().stream().findFirst().orElse(0L);
                        symbolDetailListMap = bhServerRepository.getSymbolMap(new ArrayList<>(exchangeIdSet), orgId)
                            .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, longSymbolListEntry ->
                                longSymbolListEntry.getValue().getSymbolDetailsList()));
                    } else {
                        for (Long exchangeId : exchangeIdSet) {
                            List<SymbolDetail> symbols = bhServerRepository.getSymbolDetailList(exchangeId, 0L);
                            symbolDetailListMap.put(exchangeId, symbols);
                        }
                    }
                    symbolRepository.updateSharedExchangeInfo(symbolDetailListMap);
                }

                ConcurrentMap<Long, ConcurrentMap<QuoteIndex, Symbol>> oldBrokerSymbolSetMap =
                    symbolRepository.getBrokerSymbolSetMap();


                ConcurrentMap<Long, Broker> oldOrgBrokerMap = symbolRepository.getSupportedBrokerMap();

                Set<Long> oldOrgIdSet = oldBrokerSymbolSetMap.keySet();

                // 1. 新的broker
                // 2. 不需要的旧的broker
                // 3. broker中新的币对
                // 4. broker中不需要的旧的币对
                BrokerSymbolUpdateInfo brokerSymbolUpdateInfo = null;
                if (CollectionUtils.isNotEmpty(oldOrgIdSet)) {
                    brokerSymbolUpdateInfo = getBrokerSymbolUpdateInfo(brokerSymbolListMap, orgBrokerMap,
                        oldBrokerSymbolSetMap, oldOrgBrokerMap);
                } else {
                    Map<Long, Map<QuoteIndex, Symbol>> newBrokerSymbolSetMap = new HashMap<>();

                    // 所有的broker都是新的，所有的币对都是新的
                    List<Broker> newBrokerList = new ArrayList<>(orgBrokerMap.values());

                    for (Map.Entry<Long, ConcurrentMap<QuoteIndex, Symbol>> longConcurrentMapEntry : brokerSymbolListMap.entrySet()) {
                        Long brokerId = longConcurrentMapEntry.getKey();
                        ConcurrentMap<QuoteIndex, Symbol> symbolMap = longConcurrentMapEntry.getValue();
                        newBrokerSymbolSetMap.put(brokerId, symbolMap);
                    }
                    brokerSymbolUpdateInfo = BrokerSymbolUpdateInfo
                        .builder()
                        .newBrokerList(newBrokerList)
                        .newBrokerSymbolSetMap(newBrokerSymbolSetMap)
                        .pendingRemovedBrokerList(Collections.emptyList())
                        .pendingRemovedBrokerSymbolSetMap(Collections.emptyMap())
                        .build();
                }

                // 更新了broker列表和brokerMap
                symbolRepository.update(supportBrokerList);
                // 更新broker和对应的symbol列表
                symbolRepository.setBrokerSymbolSetMap(new ConcurrentHashMap<>(brokerSymbolListMap));
                // 更新旧的symbol相关数据
                symbolRepository.updateSymbols(totalSymbolList);
                // 更新broker相关的字典
                symbolRepository.updateBrokerSymbols(brokerSymbolListMap, orgBrokerMap);

                // 先处理broker数据，再去处理订阅数据
                handleBrokerSymbolUpdateInfo(brokerSymbolUpdateInfo);

                break;
            } catch (Exception e) {
                count++;
                log.warn("Refresh symbols info ex: ", e);
                try {
                    Thread.sleep(SLEEP_INTERVAL);
                } catch (InterruptedException ignored) {
                }
                if (count >= TRY_TIME) {
                    log.error("Try times failed: [{}]", count);
                    break;
                }
            }
        }
    }

    private BrokerSymbolUpdateInfo getBrokerSymbolUpdateInfo(ConcurrentMap<Long, ConcurrentMap<QuoteIndex, Symbol>> brokerSymbolListMap,
                                                             ConcurrentMap<Long, Broker> orgBrokerMap,
                                                             ConcurrentMap<Long, ConcurrentMap<QuoteIndex, Symbol>> oldBrokerSymbolListMap,
                                                             ConcurrentMap<Long, Broker> oldOrgBrokerMap) {
        List<Broker> newBrokerList = new ArrayList<>();
        List<Broker> pendingRemovedBrokerList = new ArrayList<>();

        // orgId
        Map<Long, Map<QuoteIndex, Symbol>> newBrokerSymbolSetMap = new HashMap<>();
        Map<Long, Map<QuoteIndex, Symbol>> pendingRemovedBrokerSymbolSetMap = new HashMap<>();

        Set<Long> curOrgIdSet = brokerSymbolListMap.keySet();
        Set<Long> oldOrgIdSet = oldBrokerSymbolListMap.keySet();
        for (Long orgId : curOrgIdSet) {
            Broker broker = orgBrokerMap.get(orgId);
            // 不包含时，broker里的symbol全是新的
            if (!oldOrgIdSet.contains(broker.getOrgId())) {
                newBrokerList.add(broker);
                newBrokerSymbolSetMap.put(broker.getOrgId(), brokerSymbolListMap.get(broker.getOrgId()));
            } else {

                // 包含时需要对比旧的symbol
                ConcurrentMap<QuoteIndex, Symbol> oldSymbolMap = oldBrokerSymbolListMap.get(broker.getOrgId());
                Map<QuoteIndex, Symbol> curSymbolMap = brokerSymbolListMap.get(broker.getOrgId());

                getNewBrokerSymbolMap(newBrokerSymbolSetMap, orgId, curSymbolMap, oldSymbolMap);
                getPendingRemovedBrokerSymbolMap(pendingRemovedBrokerSymbolSetMap, orgId, curSymbolMap, oldSymbolMap);

            }
        }

        for (Long orgId : oldOrgIdSet) {
            if (!curOrgIdSet.contains(orgId)) {
                pendingRemovedBrokerList.add(oldOrgBrokerMap.get(orgId));
                // 要移除的broker所有symbol也要移除
            }
        }

        return BrokerSymbolUpdateInfo.builder()
            .newBrokerList(newBrokerList)
            .newBrokerSymbolSetMap(newBrokerSymbolSetMap)
            .pendingRemovedBrokerList(pendingRemovedBrokerList)
            .pendingRemovedBrokerSymbolSetMap(pendingRemovedBrokerSymbolSetMap)
            .build();
    }

    private void getNewBrokerSymbolMap(Map<Long, Map<QuoteIndex, Symbol>> newBrokerSymbolSetMap,
                                       Long brokerId,
                                       Map<QuoteIndex, Symbol> curSymbolMap,
                                       Map<QuoteIndex, Symbol> oldSymbolMap) {
        Map<QuoteIndex, Symbol> newSymbolMap = newBrokerSymbolSetMap.get(brokerId);

        if (Objects.isNull(newSymbolMap)) {
            newSymbolMap = new HashMap<>();
            newBrokerSymbolSetMap.put(brokerId, newSymbolMap);
        }

        for (Map.Entry<QuoteIndex, Symbol> quoteIndexSymbolEntry : curSymbolMap.entrySet()) {
            QuoteIndex newQuoteIndex = quoteIndexSymbolEntry.getKey();
            Symbol symbol = quoteIndexSymbolEntry.getValue();
            if (!oldSymbolMap.containsKey(newQuoteIndex)) {
                newSymbolMap.put(newQuoteIndex, symbol);
            }
        }
    }

    private void getPendingRemovedBrokerSymbolMap(Map<Long, Map<QuoteIndex, Symbol>> pendingRemovedBrokerSymbolSetMap,
                                                  Long brokerId,
                                                  Map<QuoteIndex, Symbol> curSymbolMap,
                                                  Map<QuoteIndex, Symbol> oldSymbolMap) {
        Map<QuoteIndex, Symbol> pendingRemovedBrokerSymbolMap = pendingRemovedBrokerSymbolSetMap.get(brokerId);
        if (Objects.isNull(pendingRemovedBrokerSymbolMap)) {
            pendingRemovedBrokerSymbolMap = new HashMap<>();
            pendingRemovedBrokerSymbolSetMap.put(brokerId, pendingRemovedBrokerSymbolMap);
        }

        for (Map.Entry<QuoteIndex, Symbol> quoteIndexSymbolEntry : oldSymbolMap.entrySet()) {
            QuoteIndex newQuoteIndex = quoteIndexSymbolEntry.getKey();
            Symbol symbol = quoteIndexSymbolEntry.getValue();
            // 不包含时，添加到要移除的列表
            if (!curSymbolMap.containsKey(newQuoteIndex)) {
                pendingRemovedBrokerSymbolMap.put(newQuoteIndex, symbol);
            }
        }
    }

    /**
     * 1. 处理broker及对应的币对订阅和取消
     * 2. broker相关topic的listener的创建和删除
     */
    private void handleBrokerSymbolUpdateInfo(BrokerSymbolUpdateInfo brokerSymbolUpdateInfo) {
        List<Broker> newBrokerList = brokerSymbolUpdateInfo.getNewBrokerList();

        // add new broker listener
        if (CollectionUtils.isNotEmpty(newBrokerList)) {
            for (Broker broker : newBrokerList) {
                onNewBroker(broker);
            }
        }

        List<Broker> pendingRemoveBrokerList = brokerSymbolUpdateInfo.getPendingRemovedBrokerList();

        // remove broker listener
        if (CollectionUtils.isNotEmpty(pendingRemoveBrokerList)) {
            for (Broker broker : pendingRemoveBrokerList) {
                removeBroker(broker);
            }
        }

        // add broker symbol
        Map<Long, Map<QuoteIndex, Symbol>> brokerNewSymbolListMap = brokerSymbolUpdateInfo.getNewBrokerSymbolSetMap();
        if (Objects.nonNull(brokerNewSymbolListMap)) {
            for (Map.Entry<Long, Map<QuoteIndex, Symbol>> longListEntry : brokerNewSymbolListMap.entrySet()) {
                Long orgId = longListEntry.getKey();
                Map<QuoteIndex, Symbol> quoteIndexSymbolMap = longListEntry.getValue();
                //quoteManager.addNewSymbolQuoteTicker(symbolSet);
                onNewSymbolsForBroker(orgId, quoteIndexSymbolMap);
            }
        }

        // remove broker symbol
        Map<Long, Map<QuoteIndex, Symbol>> pendingRemovedSymbolSetMap = brokerSymbolUpdateInfo.getPendingRemovedBrokerSymbolSetMap();
        if (Objects.nonNull(pendingRemovedSymbolSetMap)) {
            for (Map.Entry<Long, Map<QuoteIndex, Symbol>> longSetEntry : pendingRemovedSymbolSetMap.entrySet()) {
                Long orgId = longSetEntry.getKey();
                Map<QuoteIndex, Symbol> symbolSet = longSetEntry.getValue();
                onRemoveSymbolsForBroker(orgId, symbolSet);
            }
        }
    }


    private void onNewBroker(Broker broker) {
        log.info("Add new broker [{}]", broker.getOrgId());
        DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory)
                context.getAutowireCapableBeanFactory();
        for (int symbolType : SYMBOL_TYPE_ARRAY) {
            for (RealtimeIntervalEnum realtimeIntervalEnum : RealtimeIntervalEnum.values()){
                TopNListener topNListener = new TopNListener(broker.getOrgId(), TopicEnum.topN,
                        TopicEnum.topN.listenerClazz, this.scheduledExecutorService, symbolType, context);
                String beanName = BeanUtils.getBrokerTopNBeanName(broker.getOrgId(), symbolType, realtimeIntervalEnum.getInterval());
                if (!beanFactory.containsBeanDefinition(beanName)) {
                    log.info("Add topNListener [{}]", beanName);
                    beanFactory.registerSingleton(beanName, topNListener);
                    topNListener.watching();
                }

                BrokerListener brokerListener = new BrokerListener(broker.getOrgId(), TopicEnum.broker,
                        TopicEnum.broker.listenerClazz, this.scheduledExecutorService, symbolType, context);
                beanName = BeanUtils.getBrokerBeanName(broker.getOrgId(), symbolType, realtimeIntervalEnum.getInterval());
                if (!beanFactory.containsBeanDefinition(beanName)) {
                    log.info("Add brokerListener [{}]", beanName);
                    beanFactory.registerSingleton(beanName, brokerListener);
                    brokerListener.watching();
                }

                SlowBrokerListener slowBrokerListener = new SlowBrokerListener(broker.getOrgId(), TopicEnum.slowBroker,
                        TopicEnum.slowBroker.listenerClazz, this.scheduledExecutorService, symbolType, context);
                beanName = BeanUtils.getSlowBrokerBeanName(broker.getOrgId(), symbolType, realtimeIntervalEnum.getInterval());
                if (!beanFactory.containsBeanDefinition(beanName)) {
                    log.info("Add slowBrokerListener [{}]", beanName);
                    beanFactory.registerSingleton(beanName, slowBrokerListener);
                    slowBrokerListener.watching();
                }
            }
        }
    }

    private void removeBroker(Broker broker) {
        log.info("Remove broker [{}]", broker.getOrgId());
        DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory)
            context.getAutowireCapableBeanFactory();
        for (int symbolType : SYMBOL_TYPE_ARRAY) {
            for (RealtimeIntervalEnum realtimeIntervalEnum : RealtimeIntervalEnum.values()) {
                String beanName = BeanUtils.getBrokerTopNBeanName(broker.getOrgId(), symbolType, realtimeIntervalEnum.getInterval());
                if (beanFactory.containsBeanDefinition(beanName)) {
                    log.info("Remove topNListener [{}]", beanName);
                    beanFactory.removeBeanDefinition(beanName);
                }

                beanName = BeanUtils.getBrokerBeanName(broker.getOrgId(), symbolType, realtimeIntervalEnum.getInterval());
                if (beanFactory.containsBeanDefinition(beanName)) {
                    log.info("Remove brokerListener [{}]", beanName);
                    beanFactory.removeBeanDefinition(beanName);
                }

                beanName = BeanUtils.getSlowBrokerBeanName(broker.getOrgId(), symbolType, realtimeIntervalEnum.getInterval());
                if (beanFactory.containsBeanDefinition(beanName)) {
                    log.info("Remove slowBrokerListener [{}]", beanName);
                    beanFactory.removeBeanDefinition(beanName);
                }
            }
        }
    }

    private void onNewSymbolsForBroker(Long orgId, Map<QuoteIndex, Symbol> symbolSet) {
        //String beanName = BeanUtils.getBrokerTopNBeanName(orgId);
        //TopNListener topNListener = context.getBean(beanName, TopNListener.class);
        //topNListener.addNewSymbol();
    }

    private void onRemoveSymbolsForBroker(Long orgId, Map<QuoteIndex, Symbol> symbolSet) {
        for (int symbolType : SYMBOL_TYPE_ARRAY) {
            for (RealtimeIntervalEnum realtimeIntervalEnum : RealtimeIntervalEnum.values()) {
                String beanName = BeanUtils.getBrokerTopNBeanName(orgId, symbolType, realtimeIntervalEnum.getInterval());
                try {
                    TopNListener topNListener = context.getBean(beanName, TopNListener.class);
                    topNListener.removeSymbols(symbolSet.values());
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }

                try {
                    beanName = BeanUtils.getBrokerBeanName(orgId, symbolType, realtimeIntervalEnum.getInterval());
                    BrokerListener brokerListener = context.getBean(beanName, BrokerListener.class);
                    brokerListener.removeSymbols(symbolSet.values());
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }

                try {
                    beanName = BeanUtils.getSlowBrokerBeanName(orgId, symbolType, realtimeIntervalEnum.getInterval());
                    SlowBrokerListener slowBrokerListener = context.getBean(beanName, SlowBrokerListener.class);
                    slowBrokerListener.removeSymbols(symbolSet.values());
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

}
