package io.bhex.broker.quote.client;

import io.bhex.base.quote.QuoteRequest;
import io.bhex.broker.grpc.basic.Symbol;
import io.bhex.broker.grpc.gateway.GetRealQuoteEngineAddressResponse;
import io.bhex.broker.grpc.gateway.RealQuoteEngineAddress;
import io.bhex.broker.quote.bean.ReconnectionInfo;
import io.bhex.broker.quote.bean.Subscription;
import io.bhex.broker.quote.bean.SubscriptionStatus;
import io.bhex.broker.quote.client.task.AbstractSubscribeTask;
import io.bhex.broker.quote.client.task.SubscribeDepthTask;
import io.bhex.broker.quote.client.task.SubscribeIndexKlineTask;
import io.bhex.broker.quote.client.task.SubscribeKlineTask;
import io.bhex.broker.quote.client.task.SubscribeTickerTask;
import io.bhex.broker.quote.client.task.SubscribeTradeTask;
import io.bhex.broker.quote.config.ConfigCenter;
import io.bhex.broker.quote.data.QuoteIndex;
import io.bhex.broker.quote.enums.RealtimeIntervalEnum;
import io.bhex.broker.quote.enums.TopicEnum;
import io.bhex.broker.quote.job.PlatformTask;
import io.bhex.broker.quote.listener.DataListener;
import io.bhex.broker.quote.listener.v2.BookTickerListener;
import io.bhex.broker.quote.listener.v2.DepthListener;
import io.bhex.broker.quote.listener.v2.IExchangeSymbolListener;
import io.bhex.broker.quote.listener.v2.KlineListener;
import io.bhex.broker.quote.listener.v2.RealTimeListener;
import io.bhex.broker.quote.listener.v2.TicketListener;
import io.bhex.broker.quote.metrics.PushMetrics;
import io.bhex.broker.quote.repository.BrokerRepository;
import io.bhex.broker.quote.repository.DataServiceRepository;
import io.bhex.broker.quote.repository.SymbolRepository;
import io.bhex.broker.quote.util.BeanUtils;
import io.bhex.broker.quote.util.BrokerAuthUtil;
import io.bhex.broker.quote.util.NetUtil;
import io.bhex.ex.quote.Market;
import io.bhex.ex.quote.NodeInfo;
import io.bhex.ex.quote.PartitionListReply;
import io.bhex.ex.quote.PartitionMarketInfoReply;
import io.bhex.ex.quote.core.enums.CommandType;
import io.bhex.exchange.enums.KlineIntervalEnum;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static ch.qos.logback.core.CoreConstants.DOT;
import static io.bhex.broker.quote.enums.TopicEnum.diffMergedDepth;
import static io.bhex.broker.quote.enums.TopicEnum.indexKline;
import static io.bhex.broker.quote.enums.TopicEnum.kline;
import static io.bhex.broker.quote.enums.TopicEnum.markPrice;
import static io.bhex.broker.quote.enums.TopicEnum.mergedDepth;
import static io.bhex.broker.quote.enums.TopicEnum.realtimes;
import static io.bhex.broker.quote.enums.TopicEnum.trade;
import static io.bhex.ex.quote.core.enums.CommandType.DEPTH;
import static io.bhex.ex.quote.core.enums.CommandType.INDEX_KLINE;
import static io.bhex.ex.quote.core.enums.CommandType.KLINE;
import static io.bhex.ex.quote.core.enums.CommandType.REAL_TIME;
import static io.bhex.ex.quote.core.enums.CommandType.TICKET;

@Component
@Slf4j
public class QuoteInitializeManager extends Thread {
    private static final CommandType[] COMMON_SUBSCRIBED_TYPE = new CommandType[]{KLINE, REAL_TIME, TICKET, DEPTH};
    // client ID
    @Getter
    private static Long CID = System.currentTimeMillis() + NetUtil.getIp();

    private final SymbolRepository symbolRepository;
    private final BrokerRepository brokerRepository;
    private final DataServiceRepository dataServiceRepository;
    private final ApplicationContext context;
    private final ExecutorService executorService;
    private final ConfigCenter configCenter;
    private final PlatformTask platformTask;

    private volatile boolean stopped = false;

    // exchangeId -> symbolId -> partitionName
    private ConcurrentMap<Long, ConcurrentMap<String, NodeInfo>> exSymbolNodeInfoMap = new ConcurrentHashMap<>();
    private ConcurrentMap<String, NodeInfo> partitionNameNodeInfoMap = new ConcurrentHashMap<>();

    // 连接quote-engine的client
    // partitionName -> channelPool
    private ConcurrentMap<String, QuoteEngineClient> quoteEngineClientMap = new ConcurrentHashMap<>();

    // 要由quoteManager来管理
    @Getter
    private ConcurrentMap<String, ConcurrentMap<QuoteStreamIndex, Subscription>> subscriptionStatusMap = new ConcurrentHashMap<>();

    private ConcurrentHashMap<String, Boolean> subscriptionTaskStatusMap = new ConcurrentHashMap<>();

    private final BlockingDeque<QuoteEvent> quoteEventQueue = new LinkedBlockingDeque<>(10000);

    private static final String FUTURES_PARTITION_NAME = "currency-transaction";

    private final ScheduledExecutorService scheduledExecutorService;

    @Autowired
    public QuoteInitializeManager(SymbolRepository symbolRepository,
                                  DataServiceRepository dataServiceRepository,
                                  ApplicationContext context,
                                  @Qualifier("quoteServerPool") ExecutorService executorService,
                                  BrokerRepository brokerRepository,
                                  PlatformTask platformTask,
                                  @Qualifier("scheduledTaskExecutor") ScheduledExecutorService scheduledExecutorService) {
        super("QuoteInitialManagerThread");
        this.symbolRepository = symbolRepository;
        this.dataServiceRepository = dataServiceRepository;
        this.context = context;
        this.configCenter = context.getBean(ConfigCenter.class);
        this.executorService = executorService;
        this.brokerRepository = brokerRepository;
        this.platformTask = platformTask;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    public void init() {
        log.info("Client id is [{}]", CID);
        log.info("Init quote initial manager");
        getQuoteEngineRouterInfo();
        log.info("End init quote initial manager");
    }

    @Scheduled(initialDelay = 10000L, fixedDelay = 10000L)
    public void showQueueSize() {
        log.info("Manager queue size: [{}]", this.quoteEventQueue.size());
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    QuoteEvent quoteEvent = quoteEventQueue.take();
                    handleQuoteEvent(quoteEvent);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void addQuoteEvent(QuoteEvent quoteEvent) {
        enqueue(quoteEvent);
    }

    public void addHeadQuoteEvent(QuoteEvent quoteEvent) {
        try {
            quoteEventQueue.putFirst(quoteEvent);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }

    public NodeInfo getNodeInfo(Long exchangeId, String symbol) {
        return exSymbolNodeInfoMap.getOrDefault(exchangeId, new ConcurrentHashMap<>()).getOrDefault(symbol, null);
    }

    private void handleQuoteEvent(QuoteEvent quoteEvent) {
        if (stopped) {
            return;
        }
        switch (quoteEvent.getType()) {
            case SUBSCRIBED:
                String partitionName = (String) quoteEvent.getData();
                ConcurrentMap<QuoteStreamIndex, Subscription> subscriptionConcurrentMap = subscriptionStatusMap.get(partitionName);
                if (Objects.nonNull(subscriptionConcurrentMap)) {
                    QuoteStreamIndex quoteStreamIndex = quoteEvent.getQuoteStreamIndex();
                    Subscription subscription = subscriptionConcurrentMap.get(quoteEvent.getQuoteStreamIndex());
                    if (Objects.nonNull(subscription)) {
                        subscription.setSubscriptionStatus(SubscriptionStatus.SUBSCRIBED);
                        log.info("Manager Subscribed [{}:{}:{}]", quoteStreamIndex.getExchangeId(),
                            quoteStreamIndex.getSymbol(), quoteStreamIndex.getCommandType());
                    }
                }
                break;
            case UNSUBSCRIBED:
                partitionName = (String) quoteEvent.getData();
                subscriptionConcurrentMap = subscriptionStatusMap.get(partitionName);
                if (Objects.nonNull(subscriptionConcurrentMap)) {
                    QuoteStreamIndex quoteStreamIndex = quoteEvent.getQuoteStreamIndex();
                    Subscription subscription = subscriptionConcurrentMap.get(quoteEvent.getQuoteStreamIndex());
                    if (Objects.nonNull(subscription)) {
                        subscription.setSubscriptionStatus(SubscriptionStatus.UNSUBSCRIBED);
                        log.info("Manager unsubscribed [{}:{}:{}]", quoteStreamIndex.getExchangeId(),
                            quoteStreamIndex.getSymbol(), quoteStreamIndex.getCommandType());
                        unregisterBean(quoteStreamIndex.getExchangeId(), quoteStreamIndex.getSymbol(),
                            quoteStreamIndex.getCommandType());
                    }
                }
                break;
            case REFRESH_INDEX_SUBSCRIPTION:
                refreshIndexSubscription();
                break;
            case REFRESH_SUBSCRIPTION:
                refreshSubscription();
                break;
            case REFRESH_ROUTER_INFO:
                getQuoteEngineRouterInfo();
                break;
            case RECONNECT:
                ReconnectionInfo reconnectionInfo = (ReconnectionInfo) quoteEvent.getData();
                reconnectToServer(reconnectionInfo);
                break;

            default:
        }
    }

    private void reconnectToServer(ReconnectionInfo reconnectionInfo) {
        try {
            QuoteEngineClient quoteEngineClient = new QuoteEngineClient(reconnectionInfo.getHost(), reconnectionInfo.getPort(),
                reconnectionInfo.getPartitionName(), context);
            ConcurrentMap<QuoteStreamIndex, Subscription> subMap = subscriptionStatusMap.get(reconnectionInfo.getPartitionName());
            QuoteEngineClient curQuoteEngineClient = quoteEngineClientMap.get(reconnectionInfo.getPartitionName());
            // 正常重连时，只有发现和之前要重连的数据一样时，再重连，不然就不需要重连，因为加载了新的分片
            if (isEqualRouterInfo(quoteEngineClient, curQuoteEngineClient)
                && curQuoteEngineClient.isReconnect()) {
                // 如果当前的已经连接成功
                if (curQuoteEngineClient.isActive()) {
                    log.info("[{}:{}:{}] already connected ",
                        reconnectionInfo.getPartitionName(),
                        reconnectionInfo.getHost(),
                        reconnectionInfo.getPort());
                    return;
                }
                if (Objects.nonNull(subMap)) {
                    for (Map.Entry<QuoteStreamIndex, Subscription> quoteStreamIndexSubscriptionEntry : subMap.entrySet()) {
                        QuoteStreamIndex quoteStreamIndex = quoteStreamIndexSubscriptionEntry.getKey();
                        Subscription subscription = quoteStreamIndexSubscriptionEntry.getValue();
                        quoteEngineClient.queueSubscription(quoteStreamIndex);
                        subscription.setSubscriptionStatus(SubscriptionStatus.UNSUBSCRIBED);
                    }
                }
                quoteEngineClientMap.put(reconnectionInfo.getPartitionName(), quoteEngineClient);
                log.info("ReStart client [{}:{}:{}]",
                    reconnectionInfo.getPartitionName(),
                    reconnectionInfo.getHost(),
                    reconnectionInfo.getPort());
                quoteEngineClient.start();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private boolean isEqualRouterInfo(QuoteEngineClient client1, QuoteEngineClient client2) {
        if (!StringUtils.equalsIgnoreCase(client1.getHost(), client2.getHost())) {
            return false;
        }

        return client1.getPort() == client2.getPort();
    }

    // 获取index列表，分片
    public void refreshIndexSubscription() {
        long start = System.currentTimeMillis();
        log.info("Start refresh indexSubscription");
        List<String> indexSymbolList = platformTask.getIndexSymbolList();
        if (CollectionUtils.isEmpty(indexSymbolList)) {
            log.info("No index symbol");
            return;
        }

        // 1. 当前已经订阅的
        // 2. 当前需要订阅的
        Map<QuoteStreamIndex, QuoteEngineClient> needSubscribed = new HashMap<>();
        Set<QuoteStreamIndex> alreadySubscribed = new HashSet<>();
        Set<QuoteIndex> needSubscribedSymbol = new HashSet<>();
        for (String symbol : indexSymbolList) {
            Long exchangeId = 0L;
            QuoteStreamIndex quoteStreamIndex = QuoteStreamIndex
                .builder()
                .exchangeId(exchangeId)
                .symbol(symbol)
                .commandType(INDEX_KLINE)
                .build();
            QuoteEngineClient quoteEngineClient = quoteEngineClientMap.get(FUTURES_PARTITION_NAME);
            // 如果没有结点
            if (Objects.isNull(quoteEngineClient)) {
                log.error("[{}:{}:{}] 's server is not exist.",
                    quoteStreamIndex.getExchangeId(),
                    quoteStreamIndex.getSymbol(),
                    quoteStreamIndex.getCommandType());
                continue;
            }
            // 如果没有就加一个map
            ConcurrentMap<QuoteStreamIndex, Subscription> subMap = subscriptionStatusMap
                .computeIfAbsent(FUTURES_PARTITION_NAME, (pn) -> new ConcurrentHashMap<>());
            Subscription subscription = subMap.get(quoteStreamIndex);
            if (Objects.isNull(subscription)) {
                subMap.put(quoteStreamIndex,
                    Subscription.builder()
                        .subscriptionStatus(SubscriptionStatus.UNSUBSCRIBED)
                        .build());
                needSubscribed.put(quoteStreamIndex, quoteEngineClient);
                needSubscribedSymbol.add(QuoteIndex.builder()
                    .exchangeId(exchangeId)
                    .symbol(symbol)
                    .build());
            } else if (SubscriptionStatus.UNSUBSCRIBED.equals(subscription.getSubscriptionStatus())) {
                needSubscribed.put(quoteStreamIndex, quoteEngineClient);
                needSubscribedSymbol.add(QuoteIndex.builder()
                    .exchangeId(exchangeId)
                    .symbol(symbol)
                    .build());
            } else {
                alreadySubscribed.add(quoteStreamIndex);
            }
        }

        // 3. 取消不需要订阅的
        Set<QuoteStreamIndex> needUnsubscribed = new HashSet<>();
        ConcurrentMap<QuoteStreamIndex, Subscription> subscriptionMap = this.subscriptionStatusMap.get(FUTURES_PARTITION_NAME);
        for (Map.Entry<QuoteStreamIndex, Subscription> quoteStreamIndexSubscriptionEntry
            : subscriptionMap.entrySet()) {
            QuoteStreamIndex quoteStreamIndex = quoteStreamIndexSubscriptionEntry.getKey();
            if (quoteStreamIndex.getCommandType() != INDEX_KLINE) {
                continue;
            }
            if (needSubscribed.containsKey(quoteStreamIndex)) {
                continue;
            }
            if (alreadySubscribed.contains(quoteStreamIndex)) {
                continue;
            }
            Subscription subscription = quoteStreamIndexSubscriptionEntry.getValue();
            if (SubscriptionStatus.SUBSCRIBED.equals(subscription.getSubscriptionStatus())) {
                needUnsubscribed.add(quoteStreamIndex);
            }
        }

        // 注册bean到spring
        registerIndexKlineBean(needSubscribedSymbol);
        // 订阅topic级别的
        subscribe(needSubscribed);
        unsubscribe(needUnsubscribed);
        log.info("Add new indexKline subscription [{}], unsubscribe size [{}]",
            needSubscribed.size(), needUnsubscribed.size());
        log.info("End Refresh indexSubscription time [{}]", System.currentTimeMillis() - start);
    }

    //获取broker集合，symbol集合，结合共享关系，检查当前已经订阅的和没有订阅的进行处理
    public void refreshSubscription() {
        long start = System.currentTimeMillis();
        log.info("Start refresh subscription");
        Map<Long, Set<String>> allSymbolMap = symbolRepository.getAllExchangeSymbolsMap();

        // 1. 当前已经订阅的
        // 2. 当前需要订阅的
        Map<QuoteStreamIndex, QuoteEngineClient> needSubscribed = new HashMap<>();
        Set<QuoteStreamIndex> alreadySubscribed = new HashSet<>();
        Set<QuoteIndex> needSubscribedSymbol = new HashSet<>();
        for (Map.Entry<Long, Set<String>> longSetEntry : allSymbolMap.entrySet()) {
            Long exchangeId = longSetEntry.getKey();
            Set<String> symbols = longSetEntry.getValue();
            for (String symbol : symbols) {
                Long sharedExchangeId = symbolRepository.getSharedExchangeId(exchangeId, symbol);
                for (CommandType commandType : COMMON_SUBSCRIBED_TYPE) {
                    QuoteStreamIndex quoteStreamIndex = QuoteStreamIndex
                        .builder()
                        .exchangeId(sharedExchangeId)
                        .symbol(symbol)
                        .commandType(commandType)
                        .build();
                    NodeInfo nodeInfo = getNodeInfo(sharedExchangeId, symbol);
                    if (Objects.isNull(nodeInfo)) {
                        log.error("There is no partition server of [{}:{}:{}]",
                            sharedExchangeId, symbol, commandType);
                        continue;
                    }
                    QuoteEngineClient quoteEngineClient = quoteEngineClientMap.get(nodeInfo.getPartitionName());
                    // 如果没有结点
                    if (Objects.isNull(quoteEngineClient)) {
                        log.error("[{}:{}:{}] 's server is not exist.",
                            quoteStreamIndex.getExchangeId(),
                            quoteStreamIndex.getSymbol(),
                            quoteStreamIndex.getCommandType());
                        continue;
                    }
                    // 如果没有就加一个map
                    ConcurrentMap<QuoteStreamIndex, Subscription> subMap = subscriptionStatusMap
                        .computeIfAbsent(nodeInfo.getPartitionName(), (partitionName) -> new ConcurrentHashMap<>());
                    Subscription subscription = subMap.get(quoteStreamIndex);
                    if (Objects.isNull(subscription)) {
                        subMap.put(quoteStreamIndex,
                            Subscription.builder()
                                .subscriptionStatus(SubscriptionStatus.UNSUBSCRIBED)
                                .build());
                        needSubscribed.put(quoteStreamIndex, quoteEngineClient);
                        needSubscribedSymbol.add(QuoteIndex.builder()
                            .exchangeId(sharedExchangeId)
                            .symbol(symbol)
                            .build());
                    } else if (SubscriptionStatus.UNSUBSCRIBED.equals(subscription.getSubscriptionStatus())) {
                        needSubscribed.put(quoteStreamIndex, quoteEngineClient);
                        needSubscribedSymbol.add(QuoteIndex.builder()
                            .exchangeId(sharedExchangeId)
                            .symbol(symbol)
                            .build());
                    } else {
                        alreadySubscribed.add(quoteStreamIndex);
                    }
                }
            }
        }

        // 3. 取消不需要订阅的
        Set<QuoteStreamIndex> needUnsubscribed = new HashSet<>();
        for (Map.Entry<String, ConcurrentMap<QuoteStreamIndex, Subscription>> stringConcurrentMapEntry
            : this.subscriptionStatusMap.entrySet()) {
            ConcurrentMap<QuoteStreamIndex, Subscription> subscriptionConcurrentMap = stringConcurrentMapEntry.getValue();
            for (Map.Entry<QuoteStreamIndex, Subscription> quoteStreamIndexSubscriptionEntry : subscriptionConcurrentMap.entrySet()) {
                QuoteStreamIndex quoteStreamIndex = quoteStreamIndexSubscriptionEntry.getKey();
                if (CommandType.INDEX_KLINE.equals(quoteStreamIndex.getCommandType())) {
                    continue;
                }
                if (needSubscribed.containsKey(quoteStreamIndex)) {
                    continue;
                }
                if (alreadySubscribed.contains(quoteStreamIndex)) {
                    continue;
                }
                Subscription subscription = quoteStreamIndexSubscriptionEntry.getValue();
                if (SubscriptionStatus.SUBSCRIBED.equals(subscription.getSubscriptionStatus())) {
                    needUnsubscribed.add(quoteStreamIndex);
                }
            }
        }

        log.info("compare time [{}]", System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        // 注册bean到spring
        registerBean(needSubscribedSymbol);
        log.info("register bean time [{}]", System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        // 订阅topic级别的
        subscribe(needSubscribed);
        log.info("subscribe quote time [{}]", System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        unsubscribe(needUnsubscribed);
        log.info("unsubscribe quote time [{}]", System.currentTimeMillis() - start);
    }

    /**
     * 获取并更新quote-engine的路由信息和服务状态
     * 1. 获取分片信息
     * 2. 获取单个分片支持的币对列表
     * 3. 连接server
     */
    private void getQuoteEngineRouterInfo() {
        log.info("Start refresh quote engine router info.");
        try {
            //TODO 非gateway模式时，获取的orgId是0，分区等后续应该需要迁移或重新规划
            Long orgId = BrokerAuthUtil.getOrgIdSet().stream().findFirst().orElse(0L);
            PartitionListReply reply = dataServiceRepository.getPartitionNameList(orgId);
            for (String partitionName : reply.getPartitionNameList()) {

                try {
                    PartitionMarketInfoReply marketInfoReply;
                    String appName = configCenter.getPartitionMap().get(partitionName);
                    marketInfoReply = dataServiceRepository.getPartitionMarketInfo0(appName, partitionName, orgId);
                    NodeInfo nodeInfo = getLBAddress(marketInfoReply.getNodeInfo());
                    partitionNameNodeInfoMap.put(nodeInfo.getPartitionName(), nodeInfo);

                    // 当路由有变化时
                    // 1. close当前client
                    // 2. 连接新的client
                    // 3. 订阅此分片上的币对
                    quoteEngineClientMap.computeIfPresent(marketInfoReply.getPartitionName(),
                        (parName, pool) -> {
                            if (!StringUtils.equals(pool.getHost(), nodeInfo.getAddress())
                                || pool.getPort() != nodeInfo.getPort()) {
                                log.info("Shutdown old quote-engine connection [{}:{}]", pool.getHost(), pool.getPort());
                                pool.close(false);
                                QuoteEngineClient quoteEngineClient = initQuoteEngineClient(nodeInfo);
                                log.info("Start client [{}:{}:{}]",
                                    nodeInfo.getPartitionName(),
                                    nodeInfo.getAddress(),
                                    nodeInfo.getPort());
                                quoteEngineClient.start();
                                return quoteEngineClient;
                            }
                            return pool;
                        });

                    quoteEngineClientMap.computeIfAbsent(marketInfoReply.getPartitionName(),
                        (parName) -> {
                            QuoteEngineClient quoteEngineClient = initQuoteEngineClient(nodeInfo);
                            quoteEngineClient.start();
                            return quoteEngineClient;
                        });

                    for (Market market : marketInfoReply.getMarketList()) {
                        ConcurrentMap<String, NodeInfo> nodeInfoConcurrentMap =
                            exSymbolNodeInfoMap
                                .computeIfAbsent(market.getExchangeId(), ex -> new ConcurrentHashMap<>());
                        nodeInfoConcurrentMap.put(market.getSymbolId(), nodeInfo);
                    }

                    log.info("Load partitionName [{}] node info [{}:{}] ",
                        partitionName, nodeInfo.getAddress(), nodeInfo.getPort());

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    continue;
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            scheduledExecutorService.schedule(() -> addQuoteEvent(QuoteEvent.builder()
                .type(QuoteEventTypeEnum.REFRESH_ROUTER_INFO)
                .build()), 5, TimeUnit.SECONDS);
        }
        log.info("End refresh quote engine router info.");
    }

    private NodeInfo getLBAddress(NodeInfo nodeInfo) {
        if (StringUtils.isNotEmpty(configCenter.getPlatform())) {
            try {
                GetRealQuoteEngineAddressResponse response = brokerRepository
                    .getQuoteEngineAddress(nodeInfo.getAddress(), nodeInfo.getPort(), configCenter.getPlatform());
                RealQuoteEngineAddress address = response.getAddress();
                nodeInfo = nodeInfo.toBuilder()
                    .setAddress(address.getRealHost())
                    .setPort(address.getRealPort())
                    .build();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        return nodeInfo;
    }

    /**
     * 初始化quote-engine-client
     *
     * @param nodeInfo quote-engine info
     * @return quote-engine-client
     */
    QuoteEngineClient initQuoteEngineClient(NodeInfo nodeInfo) {
        QuoteEngineClient quoteEngineClient = new QuoteEngineClient(nodeInfo.getAddress(), nodeInfo.getPort(),
            nodeInfo.getPartitionName(),
            context);

        ConcurrentMap<QuoteStreamIndex, Subscription> map = subscriptionStatusMap.get(nodeInfo.getPartitionName());
        if (Objects.nonNull(map)) {
            for (Map.Entry<QuoteStreamIndex, Subscription> quoteStreamIndexSubscriptionEntry : map.entrySet()) {
                Subscription subscription = quoteStreamIndexSubscriptionEntry.getValue();
                QuoteStreamIndex quoteStreamIndex = quoteStreamIndexSubscriptionEntry.getKey();
                quoteEngineClient.queueSubscription(quoteStreamIndex);
                subscription.setSubscriptionStatus(SubscriptionStatus.UNSUBSCRIBED);
            }
        } else {
            subscriptionStatusMap.put(nodeInfo.getPartitionName(), new ConcurrentHashMap<>());
        }
        return quoteEngineClient;
    }

    @Scheduled(initialDelay = 5000L, fixedDelay = 30000L)
    public void refreshIndexSymbolInfo() {
        log.info("Add refresh index symbol task");
        addQuoteEvent(QuoteEvent.builder()
            .type(QuoteEventTypeEnum.REFRESH_INDEX_SUBSCRIPTION)
            .build());
    }

    public void markShutdown() {
        this.stopped = true;
    }

    public void shutdown() {
        log.info("QuoteInitializeManager stopping");

        // 消除队列任务
        while (!this.quoteEventQueue.isEmpty()) {
            this.quoteEventQueue.clear();
        }

        // 断开 engine的连接
        for (Map.Entry<String, QuoteEngineClient> stringQuoteEngineClientEntry : quoteEngineClientMap.entrySet()) {
            stringQuoteEngineClientEntry.getValue().close(false);
            log.info("Close [{}]", stringQuoteEngineClientEntry.getKey());
        }
        log.info("QuoteInitializeManager stopped");
    }

    void onSubscribed(Long exchangeId, String symbol, CommandType commandType) {
        log.info("Subscribed [{}:{}:{}]", exchangeId, symbol, commandType);
    }

    private void subscribeQuote(List<Runnable> taskList) {
        executorService.execute(() -> {
            try {
                if (stopped) {
                    return;
                }

                for (Runnable callable : taskList) {
                    if (!(callable instanceof AbstractSubscribeTask)) {
                        continue;
                    }
                    AbstractSubscribeTask abstractSubscribeTask = (AbstractSubscribeTask) callable;
                    String taskKey = getTaskKey(abstractSubscribeTask);

                    subscriptionTaskStatusMap.computeIfPresent(taskKey, (key, v) -> {
                        if (Boolean.TRUE.equals(v)) {
                            return Boolean.TRUE;
                        }
                        executorService.execute(() -> {
                            try {
                                abstractSubscribeTask.run();
                            } finally {
                                subscriptionTaskStatusMap.put(key, Boolean.FALSE);
                            }
                        });
                        return Boolean.TRUE;
                    });
                    subscriptionTaskStatusMap.computeIfAbsent(taskKey, (key) -> {
                        executorService.submit(() -> {
                            try {
                                abstractSubscribeTask.run();
                            } finally {
                                subscriptionTaskStatusMap.put(key, Boolean.FALSE);
                            }
                        });
                        return Boolean.TRUE;
                    });

                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        });
    }

    private String getTaskKey(AbstractSubscribeTask task) {
        return StringUtils.EMPTY + task.getExchangeId()
            + task.getSymbol()
            + task.getCommandType();
    }

    private void unsubscribe(Set<QuoteStreamIndex> quoteStreamIndices) {
        for (QuoteStreamIndex quoteStreamIndex : quoteStreamIndices) {
            NodeInfo nodeInfo;
            if (quoteStreamIndex.getExchangeId() == 0) {
                // 指数k线专用
                nodeInfo = partitionNameNodeInfoMap.get(FUTURES_PARTITION_NAME);
            } else {
                nodeInfo = getNodeInfo(quoteStreamIndex.getExchangeId(), quoteStreamIndex.getSymbol());
            }
            if (nodeInfo == null) {
                log.error("Node info of [{}:{}] is null", quoteStreamIndex.getExchangeId(), quoteStreamIndex.getSymbol());
                continue;
            }
            QuoteEngineClient client = quoteEngineClientMap.get(nodeInfo.getPartitionName());
            if (client != null && client.isActive()) {
                log.info("Unsubscribe [{}:{}:{}]", quoteStreamIndex.getExchangeId(),
                    quoteStreamIndex.getSymbol(),
                    quoteStreamIndex.getCommandType());
                client.getQuoteResponseHandler().send(QuoteRequest.newBuilder()
                    .setExchangeId(quoteStreamIndex.getExchangeId())
                    .setSymbol(quoteStreamIndex.getSymbol())
                    .setType(quoteStreamIndex.getCommandType().getCode())
                    .setSub(false)
                    .build());

            }

            ConcurrentMap<QuoteStreamIndex, Subscription> subMap = subscriptionStatusMap.get(nodeInfo.getPartitionName());
            if (Objects.nonNull(subMap)) {
                Subscription sub = subMap.get(quoteStreamIndex);
                if (Objects.nonNull(sub)) {
                    sub.setSubscriptionStatus(SubscriptionStatus.UNSUBSCRIBED);
                }
            }
        }
    }

    public void markUnsubscribed(String partitionName, Long exchangeId, String symbol, CommandType commandType) {
        enqueue(QuoteEvent.builder()
            .quoteStreamIndex(QuoteStreamIndex.builder()
                .exchangeId(exchangeId)
                .symbol(symbol)
                .commandType(commandType)
                .build())
            .data(partitionName)
            .type(QuoteEventTypeEnum.UNSUBSCRIBED)
            .build());
    }

    public void enqueue(QuoteEvent quoteEvent) {
        try {
            quoteEventQueue.put(quoteEvent);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }

    public void markSubscribed(String partitionName, Long exchangeId, String symbol, CommandType commandType) {
        addHeadQuoteEvent(QuoteEvent.builder()
            .quoteStreamIndex(QuoteStreamIndex.builder()
                .exchangeId(exchangeId)
                .symbol(symbol)
                .commandType(commandType)
                .build())
            .data(partitionName)
            .type(QuoteEventTypeEnum.SUBSCRIBED)
            .build());
    }

    private void subscribe(Map<QuoteStreamIndex, QuoteEngineClient> quoteStreamIndices) {
        List<Runnable> taskList = new ArrayList<>();

        for (Map.Entry<QuoteStreamIndex, QuoteEngineClient> quoteStreamIndexQuoteEngineClientEntry : quoteStreamIndices.entrySet()) {
            QuoteStreamIndex quoteStreamIndex = quoteStreamIndexQuoteEngineClientEntry.getKey();
            QuoteEngineClient quoteEngineClient = quoteStreamIndexQuoteEngineClientEntry.getValue();
            taskList.add(subscribe(quoteStreamIndex, quoteEngineClient));
        }

        subscribeQuote(taskList);
    }

    private Runnable subscribe(QuoteStreamIndex quoteStreamIndex, QuoteEngineClient quoteEngineClient) {
        Long exchangeId = quoteStreamIndex.getExchangeId();
        String symbol = quoteStreamIndex.getSymbol();
        CommandType topic = quoteStreamIndex.getCommandType();
        switch (topic) {
            case KLINE:
                return klineTask(exchangeId, symbol, quoteEngineClient);
            case TICKET:
                return tradeTask(exchangeId, symbol, quoteEngineClient);
            case REAL_TIME:
                return tickerTask(exchangeId, symbol, quoteEngineClient);
            case DEPTH:
                return depthTask(exchangeId, symbol, quoteEngineClient);
            case INDEX_KLINE:
                return indexKlineTask(symbol, quoteEngineClient);
            default:
                return () -> {
                };
        }
    }

    private Runnable tradeTask(Long exchangeId, String symbol, QuoteEngineClient quoteEngineClient) {
        return new SubscribeTradeTask(exchangeId, symbol, this, context, dataServiceRepository, quoteEngineClient);
    }

    private Runnable tickerTask(Long exchangeId, String symbol, QuoteEngineClient quoteEngineClient) {
        return new SubscribeTickerTask(exchangeId, symbol, this, context, dataServiceRepository, quoteEngineClient);
    }


    private Runnable klineTask(Long exchangeId, String symbol, QuoteEngineClient quoteEngineClient) {
        return new SubscribeKlineTask(exchangeId, symbol, this, context, dataServiceRepository, quoteEngineClient);
    }

    private Runnable indexKlineTask(String symbol, QuoteEngineClient quoteEngineClient) {
        return new SubscribeIndexKlineTask(symbol, context, dataServiceRepository, quoteEngineClient);
    }

    private Runnable depthTask(Long exchangeId, String symbol, QuoteEngineClient quoteEngineClient) {
        return new SubscribeDepthTask(exchangeId, symbol, this, context, dataServiceRepository,
            symbolRepository, quoteEngineClient);
    }

    private void registerFutureBean(Set<Symbol> symbolSet) {
        for (Symbol symbol : symbolSet) {
            Long sharedExchangeId = symbolRepository.getSharedExchangeId(symbol.getExchangeId(), symbol.getSymbolId());
            registerMarkPriceBean(sharedExchangeId, symbol.getSymbolId(), markPrice);
        }
    }

    /**
     * 注册相关的bean到spring
     */
    private void registerQuoteBean(Long exchangeId, String symbol) {
        String symbols = exchangeId + (DOT + symbol);
        registerDepthBean(exchangeId, symbol);
        registerKlineBean(exchangeId, symbol, symbols);
        registerBean(exchangeId, symbol, symbols, trade);
        registerRealtimeBean(exchangeId, symbol, symbols);

        registerBeanV2(DepthListener.class, BeanUtils.getDepthV2BeanName(exchangeId, symbol),
            Arrays.asList(exchangeId, symbol));
        registerBeanV2(TicketListener.class, BeanUtils.getTradeV2BeanName(exchangeId, symbol),
            Arrays.asList(exchangeId, symbol));
        registerRealtimeBeanV2(exchangeId, symbol);
        registerBeanV2(BookTickerListener.class, BeanUtils.getBookTickerV2BeanName(exchangeId, symbol),
            Arrays.asList(exchangeId, symbol));
        registerKlineBeanV2(exchangeId, symbol);
    }

    private void registerBean(Set<QuoteIndex> quoteIndexSet) {
        for (QuoteIndex quoteIndex : quoteIndexSet) {
            registerQuoteBean(quoteIndex.getExchangeId(), quoteIndex.getSymbol());
        }
    }

    private void registerIndexKlineBean(Set<QuoteIndex> quoteIndexSet) {
        for (QuoteIndex quoteIndex : quoteIndexSet) {
            registerIndexKlineBean(quoteIndex.getSymbol());
        }
    }

    private void registerBean(Long exchangeId, String symbol, String symbols, TopicEnum topicEnum) {
        registerBean(topicEnum.listenerClazz,
            BeanUtils.getBeanNameByTopicWithExchangeIdAndSymbol(exchangeId, symbol, topicEnum)
            , Arrays.asList(symbols, topicEnum, topicEnum.listenerClazz));
    }

    private void registerMarkPriceBean(Long exchangeId, String symbol, TopicEnum topicEnum) {
        registerBean(topicEnum.listenerClazz,
            BeanUtils.getMarkPriceBeanName(exchangeId, symbol),
            Arrays.asList(exchangeId, symbol));
    }

    private void registerKlineBean(Long exchangeId, String symbol, String symbols) {
        for (KlineIntervalEnum value : KlineIntervalEnum.values()) {
            registerBean(kline.listenerClazz, BeanUtils.getKlineBeanName(exchangeId, symbol, value.interval)
                , Arrays.asList(symbols, value.interval, kline, kline.listenerClazz));
        }
    }

    private void registerRealtimeBean(Long exchangeId, String symbol, String symbols) {
        for (RealtimeIntervalEnum realtimeIntervalEnum : RealtimeIntervalEnum.values()) {
            registerBean(TopicEnum.realtimes.listenerClazz, BeanUtils.getRealTimeBeanName(exchangeId, symbol, realtimeIntervalEnum.getInterval())
                , Arrays.asList(symbols, realtimes, realtimes.listenerClazz, context));
        }
    }

    private void registerIndexKlineBean(String symbol) {
        for (KlineIntervalEnum value : KlineIntervalEnum.values()) {
            registerBean(indexKline.listenerClazz, BeanUtils.getIndexKlineBeanName(symbol, value.interval)
                , Arrays.asList(symbol, value.interval, indexKline, indexKline.listenerClazz));
        }
    }

    private void registerKlineBeanV2(Long exchangeId, String symbol) {
        for (KlineIntervalEnum value : KlineIntervalEnum.values()) {
            registerBeanV2(KlineListener.class, BeanUtils.getKlineV2BeanName(exchangeId, symbol, value.interval),
                Arrays.asList(exchangeId, symbol, value));
        }
    }

    private void registerRealtimeBeanV2(Long exchangeId, String symbol) {
        for (RealtimeIntervalEnum realtimeIntervalEnum : RealtimeIntervalEnum.values()) {
            registerBeanV2(RealTimeListener.class, BeanUtils.getTickerV2BeanName(exchangeId, symbol, realtimeIntervalEnum.getInterval())
                , Arrays.asList(exchangeId, symbol, context));
        }
    }

    private void registerDepthBean(Long exchangeId, String symbol) {
        String symbols = exchangeId + (DOT + symbol);
        registerBean(TopicEnum.depth.listenerClazz, BeanUtils.getDepthBeanName(exchangeId, symbol),
            Arrays.asList(symbols, TopicEnum.depth, TopicEnum.depth.listenerClazz));
        registerBean(TopicEnum.diffDepth.listenerClazz, BeanUtils.getDiffDepthBeanName(exchangeId, symbol),
            Arrays.asList(symbols, TopicEnum.diffDepth, TopicEnum.diffDepth.listenerClazz));
        // 合并精度的深度
        int[] mergedArray = symbolRepository.getMergedArray(symbol);
        for (int dumpScale : mergedArray) {
            registerMergedDepth(exchangeId, symbol, dumpScale, symbols);
        }
    }

    private void registerMergedDepth(Long exchangeId, String symbol, Integer dumpScale, String symbols) {
        registerBean(mergedDepth.listenerClazz, BeanUtils.getDepthBeanName(exchangeId, symbol, dumpScale),
            Arrays.asList(symbols, dumpScale, mergedDepth, mergedDepth.listenerClazz));
        registerBean(diffMergedDepth.listenerClazz, BeanUtils.getDiffDepthBeanName(exchangeId, symbol, dumpScale),
            Arrays.asList(symbols, dumpScale, diffMergedDepth, diffMergedDepth.listenerClazz));
    }

    void registerMergedDepth(Long exchangeId, String symbol, Integer dumpScale) {
        registerMergedDepth(exchangeId, symbol, dumpScale, exchangeId + (DOT + symbol));
    }

    private void registerBean(Class listenerClass, String beanName, List<Object> args) {
        DefaultListableBeanFactory defaultListableBeanFactory = (DefaultListableBeanFactory)
            context.getAutowireCapableBeanFactory();
        if (defaultListableBeanFactory.containsBeanDefinition(beanName)) {
            //log.warn("Already registered bean [{}]", beanName);
            return;
        }
        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.genericBeanDefinition(listenerClass);
        args.forEach(beanDefinitionBuilder::addConstructorArgValue);
        defaultListableBeanFactory.registerBeanDefinition(beanName, beanDefinitionBuilder.getBeanDefinition());
        DataListener dataListener = (DataListener) defaultListableBeanFactory.getBean(beanName);
        dataListener.watching();
        PushMetrics.addQuoteSpringBean(dataListener.getExchangeId(), dataListener.getSymbol());
        //log.info("Register bean [{}]", beanName);
    }

    private void registerBeanV2(Class listenerClass, String beanName, List<Object> args) {
        DefaultListableBeanFactory defaultListableBeanFactory = (DefaultListableBeanFactory)
            context.getAutowireCapableBeanFactory();
        if (defaultListableBeanFactory.containsBeanDefinition(beanName)) {
            //log.warn("Already registered v2bean [{}]", beanName);
            return;
        }
        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.genericBeanDefinition(listenerClass);
        args.forEach(beanDefinitionBuilder::addConstructorArgValue);
        defaultListableBeanFactory.registerBeanDefinition(beanName, beanDefinitionBuilder.getBeanDefinition());
        IExchangeSymbolListener topicListener = (IExchangeSymbolListener) defaultListableBeanFactory.getBean(beanName);
        topicListener.watch();
        PushMetrics.addQuoteSpringBean(topicListener.getExchangeId(), topicListener.getSymbolId());
        //log.debug("Register v2bean [{}]", beanName);
    }

    private void unregisterBean(Long exchangeId, String symbolId, CommandType commandType) {
        switch (commandType) {
            case KLINE:
                unregisterKlineBean(exchangeId, symbolId);
                break;
            case ZIP_DEPTH:
                unregisterDepthBean(exchangeId, symbolId);
                break;
            case TICKET:
                unregisterTradeBean(exchangeId, symbolId);
                break;
            case REAL_TIME:
                unregisterTickerBean(exchangeId, symbolId);
                break;
            default:
        }
    }

    private void unregisterKlineBean(Long exchangeId, String symbol) {
        BeanDefinitionRegistry registry = ((BeanDefinitionRegistry) context.getAutowireCapableBeanFactory());
        // unregister kline
        for (KlineIntervalEnum value : KlineIntervalEnum.values()) {
            String beanName = BeanUtils.getKlineBeanName(exchangeId, symbol, value.interval);
            if (registry.containsBeanDefinition(beanName)) {
                registry.removeBeanDefinition(beanName);
            }
        }
    }

    private void unregisterTradeBean(Long exchangeId, String symbol) {
        BeanDefinitionRegistry registry = ((BeanDefinitionRegistry) context.getAutowireCapableBeanFactory());
        String beanName = BeanUtils.getTradesBeanName(exchangeId, symbol);
        if (registry.containsBeanDefinition(beanName)) {
            registry.removeBeanDefinition(beanName);
        }
    }

    private void unregisterTickerBean(Long exchangeId, String symbol) {
        BeanDefinitionRegistry registry = ((BeanDefinitionRegistry) context.getAutowireCapableBeanFactory());
        for (RealtimeIntervalEnum realtimeIntervalEnum : RealtimeIntervalEnum.values()) {
            String beanName = BeanUtils.getRealTimeBeanName(exchangeId, symbol, realtimeIntervalEnum.getInterval());
            if (registry.containsBeanDefinition(beanName)) {
                registry.removeBeanDefinition(beanName);
                if (log.isDebugEnabled()) {
                    log.debug("Unregister bean [{}]", beanName);
                }
            }
        }
    }

    private void unregisterDepthBean(Long exchangeId, String symbol) {
        BeanDefinitionRegistry registry = ((BeanDefinitionRegistry) context.getAutowireCapableBeanFactory());
        String beanName = BeanUtils.getDepthBeanName(exchangeId, symbol);
        if (registry.containsBeanDefinition(beanName)) {
            registry.removeBeanDefinition(beanName);
            if (log.isDebugEnabled()) {
                log.debug("Unregister bean [{}]", beanName);
            }
        }

        beanName = BeanUtils.getDiffDepthBeanName(exchangeId, symbol);
        if (registry.containsBeanDefinition(beanName)) {
            registry.removeBeanDefinition(beanName);
            if (log.isDebugEnabled()) {
                log.debug("Unregister bean [{}]", beanName);
            }
        }
        int[] mergedArray = symbolRepository.getMergedArray(symbol);
        for (int dumpScale : mergedArray) {
            unregisterMergedDepthBean(exchangeId, symbol, dumpScale);
        }
    }

    private void unregisterMergedDepthBean(Long exchangeId, String symbol, Integer dumpScale) {
        BeanDefinitionRegistry registry = ((BeanDefinitionRegistry) context.getAutowireCapableBeanFactory());
        String beanName = BeanUtils.getDepthBeanName(exchangeId, symbol, dumpScale);
        if (registry.containsBeanDefinition(beanName)) {
            registry.removeBeanDefinition(beanName);
            if (log.isDebugEnabled()) {
                log.debug("Unregister bean [{}]", beanName);
            }
        }

        beanName = BeanUtils.getDiffDepthBeanName(exchangeId, symbol, dumpScale);
        if (registry.containsBeanDefinition(beanName)) {
            registry.removeBeanDefinition(beanName);
            if (log.isDebugEnabled()) {
                log.debug("Unregister bean [{}]", beanName);
            }
        }
    }


}
