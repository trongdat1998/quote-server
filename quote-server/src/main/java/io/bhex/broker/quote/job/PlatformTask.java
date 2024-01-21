package io.bhex.broker.quote.job;

import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.Index;
import io.bhex.base.quote.IndexConfig;
import io.bhex.base.quote.IndexConfigList;
import io.bhex.broker.quote.data.dto.IndexDTO;
import io.bhex.broker.quote.enums.OrderConstants;
import io.bhex.broker.quote.enums.TopicEnum;
import io.bhex.broker.quote.listener.DataListener;
import io.bhex.broker.quote.listener.IndexListener;
import io.bhex.broker.quote.repository.BhServerRepository;
import io.bhex.broker.quote.repository.DataServiceRepository;
import io.bhex.broker.quote.repository.QuoteFarmerRepository;
import io.bhex.broker.quote.repository.SymbolRepository;
import io.bhex.broker.quote.util.BrokerAuthUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static io.bhex.broker.quote.common.SymbolConstants.UNDERLINE;

@Service
@Slf4j
@Order(OrderConstants.ORDER_BH_SERVER)
public class PlatformTask {

    @Getter
    private List<String> indexSymbolList = new ArrayList<>();
    private final BhServerRepository bhServerRepository;
    private final DataServiceRepository dataServiceRepository;
    private final ApplicationContext context;
    private final SymbolRepository symbolRepository;
    private final QuoteFarmerRepository quoteFarmerRepository;
    private List<IndexConfig> indexConfigList = new ArrayList<>();

    /**
     * Index memory cache for api
     */
    @Getter
    private Map<String, Index> indexMap = new HashMap<>();

    @Autowired
    public PlatformTask(BhServerRepository bhServerRepository,
                        DataServiceRepository dataServiceRepository,
                        ApplicationContext context,
                        SymbolRepository symbolRepository,
                        QuoteFarmerRepository quoteFarmerRepository) {
        this.bhServerRepository = bhServerRepository;
        this.dataServiceRepository = dataServiceRepository;
        this.context = context;
        this.symbolRepository = symbolRepository;
        this.quoteFarmerRepository = quoteFarmerRepository;
    }

    @PostConstruct
    public void setUp() {
        log.info("Init bh-server info is started.");
        refreshIndexTokenList();
        log.info("Init bh-server info is ended.");
    }

    /**
     * 5分钟刷新一次指数币对
     */
    @Scheduled(initialDelay = 60 * 1000L, fixedDelay = 60 * 1000L)
    public void refreshIndexTokenList() {
        log.info("Start refreshToken list.");
        try {
            //TODO 非gateway模式时，获取的orgId是0，后期平台orgId生效时再使用所有的orgId合并结果处理
            Long orgId = BrokerAuthUtil.getOrgIdSet().stream().findFirst().orElse(0L);
            List<String> optionIndexList = bhServerRepository.getIndexTokenList(orgId);
            IndexConfigList indexConfigList = quoteFarmerRepository.getIndexConfigList(orgId);
            Set<String> indexSet = new HashSet<>(optionIndexList);
            for (IndexConfig indexConfig : indexConfigList.getIndexConfigList()) {
                indexSet.add(indexConfig.getName());
            }
            bhServerRepository.updateIndexConfig(indexConfigList.getIndexConfigList());

            this.indexSymbolList = new ArrayList<>(indexSet);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        log.info("End refreshToken list.");
    }

    @Scheduled(initialDelay = 5 * 1000L, fixedRate = 60 * 1000L)
    public void refreshPosition() {
        log.info("Start refresh position");
        try {
            Long orgId = BrokerAuthUtil.getOrgIdSet().stream().findFirst().orElse(0L);
            Set<Long> exchangeIdSet = symbolRepository.getExchangeIdSet();
            if (CollectionUtils.isNotEmpty(exchangeIdSet)) {
                for (Long exchangeId : exchangeIdSet) {
                    Map<String, String> positionMap = bhServerRepository.getTotalPosition(exchangeId, orgId);
                    bhServerRepository.updatePosition(exchangeId, positionMap);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        log.info("End refresh position");
    }


    @Scheduled(fixedRate = 1_000L)
    public void refreshIndexCache() {
        log.info("Start refresh index cache.");
        try {
            //TODO 非gateway模式时，获取的orgId是0，后期平台orgId生效时再使用所有的orgId合并结果处理
            Long orgId = BrokerAuthUtil.getOrgIdSet().stream().findFirst().orElse(0L);
            Map<String, Index> indexMap = dataServiceRepository.getIndices(this.indexSymbolList, orgId);
            if (CollectionUtils.isNotEmpty(indexMap.entrySet())) {
                for (Map.Entry<String, Index> stringIndexEntry : indexMap.entrySet()) {
                    String symbol = stringIndexEntry.getKey();
                    Index index = stringIndexEntry.getValue();
                    String beanName = TopicEnum.index.name() + UNDERLINE + symbol;
                    try {
                        IndexListener indexListener = (IndexListener) context.getBean(beanName, DataListener.class);
                        indexListener.onEngineMessage(index);
                    } catch (NoSuchBeanDefinitionException e) {
                        DefaultListableBeanFactory defaultListableBeanFactory = (DefaultListableBeanFactory) context
                            .getAutowireCapableBeanFactory();
                        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder
                            .genericBeanDefinition(TopicEnum.index.listenerClazz);
                        List<Object> args = Arrays.asList(symbol, TopicEnum.index, TopicEnum.index.listenerClazz);
                        args.forEach(beanDefinitionBuilder::addConstructorArgValue);
                        defaultListableBeanFactory.registerBeanDefinition(beanName, beanDefinitionBuilder.getBeanDefinition());
                        IndexListener dataListener = (IndexListener) defaultListableBeanFactory.getBean(beanName);
                        dataListener.watching();
                        dataListener.onEngineMessage(index);
                    }

                }
                this.indexMap = indexMap;
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        log.info("End refresh index cache.");
    }

    public Index getIndexBySymbol(String symbol) {
        return this.indexMap.get(symbol);
    }

    public Map<String, BigDecimal> getIndexBySymbolSet(Set<String> symbols) {
        Map<String, Index> indexMapCache = this.indexMap;
        Map<String, BigDecimal> indexMap = new HashMap<>();
        for (String symbol : symbols) {
            Index index = indexMapCache.get(symbol);
            if (Objects.nonNull(index)) {
                indexMap.put(symbol, DecimalUtil.toBigDecimal(index.getIndex()));
            }
        }
        return indexMap;
    }

    public Map<String, Index> getIndexMapBySymbolSet(Set<String> symbols) {
        Map<String, Index> indexMapCache = this.indexMap;
        Map<String, Index> resultMap = new HashMap<>();
        for (String symbol : symbols) {
            Index index = indexMapCache.get(symbol);
            if (Objects.nonNull(index)) {
                resultMap.put(symbol, index);
            }
        }
        return resultMap;
    }

    public Map<String, IndexDTO> getIndexDTOMapBySymbolSet(Set<String> symbols) {
        Map<String, Index> indexMap = getIndexMapBySymbolSet(symbols);
        Map<String, IndexDTO> indexDTOMap = new HashMap<>();
        if (!indexMap.isEmpty()) {
            for (Map.Entry<String, Index> stringIndexEntry : indexMap.entrySet()) {
                String k = stringIndexEntry.getKey();
                Index v = stringIndexEntry.getValue();
                indexDTOMap.put(k, IndexDTO.parse(v));
            }
        }
        return indexDTOMap;

    }

    public Map<String, Index> getAllIndices() {
        return Collections.unmodifiableMap(this.indexMap);
    }
}
