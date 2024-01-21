package io.bhex.broker.quote.util;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import io.bhex.broker.quote.repository.BrokerAuthRepository;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.context.ApplicationContext;

import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author wangsc
 * @description org认证缓存（只缓存加签串）
 * @date 2020-06-05 14:43
 */
@Slf4j
public class BrokerAuthUtil {

    private static ApplicationContext applicationContext;

    private static Set<Long> orgIdSet = ImmutableSet.of();

    private static final Cache<Long, BrokerApiAuth> BROKER_AUTH_CACHE = CacheBuilder.newBuilder()
            .expireAfterWrite(4, TimeUnit.MINUTES)
            .build();

    /**
     * 强制刷新间隔（用户orgId不存在的保护)
     */
    private static final Cache<Long, Long> RE_FRESH_INTERVAL_CACHE = CacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.MINUTES)
            .build();

    /**
     * 强制不存在刷新的保护时间5s
     */
    private static final int REFRESH_INTERVAL_MILLISECONDS = 5 * 1000;

    public static void init(ApplicationContext applicationContext) {
        BrokerAuthUtil.applicationContext = applicationContext;
        //获取当前支持的所有orgId
        refreshAllOrgIds();
        ScheduledExecutorService refreshAllOrgIdsScheduler = new ScheduledThreadPoolExecutor(1,
                new BasicThreadFactory.Builder().namingPattern("RefreshAllOrgIds_%d").daemon(false).build());
        refreshAllOrgIdsScheduler.scheduleAtFixedRate(BrokerAuthUtil::refreshAllOrgIds, 2, 2, TimeUnit.MINUTES);
    }

    /**
     * 用于定时任务的刷新（刷新一次吧）
     */
    private static void refreshAllOrgIds() {
        try {
            List<Long> orgIds = applicationContext.getBean(BrokerAuthRepository.class)
                    .getALLAuthOrgIds();
            if (orgIds.size() > 0) {
                orgIdSet = ImmutableSet.copyOf(orgIds);
            }
            log.info("currently supported org id: {}", orgIdSet);
        } catch (Exception e) {
            log.warn("get supported orgIds error! {}", e.getMessage());
        }
    }

    public static Set<Long> getOrgIdSet() {
        return orgIdSet;
    }

    /**
     * 检查获取认证
     *
     * @param orgId
     * @return
     */
    public static BrokerApiAuth getBrokerAuth(Long orgId) {
        if (orgId == null) {
            return null;
        }
        BrokerApiAuth brokerAuth = BROKER_AUTH_CACHE.getIfPresent(orgId);
        if (brokerAuth == null || brokerAuth.refreshTime < System.currentTimeMillis()) {
            return reFreshBrokerAuth(orgId);
        } else {
            return brokerAuth;
        }
    }

    private static BrokerApiAuth reFreshBrokerAuth(Long orgId) {
        Long time = RE_FRESH_INTERVAL_CACHE.getIfPresent(orgId);
        long currentTimeMillis = System.currentTimeMillis();
        if (time == null || time < currentTimeMillis) {
            //从broker-server获取认证串
            try {
                BrokerApiAuth brokerAuth = applicationContext.getBean(BrokerAuthRepository.class)
                        .getBrokerAuthByOrgId(orgId);
                if (brokerAuth != null) {
                    BROKER_AUTH_CACHE.put(orgId, brokerAuth);
                } else {
                    RE_FRESH_INTERVAL_CACHE.put(orgId, currentTimeMillis + REFRESH_INTERVAL_MILLISECONDS);
                }
                return brokerAuth;
            } catch (Exception e) {
                //连接broker-server异常
                RE_FRESH_INTERVAL_CACHE.cleanUp();
                log.warn("Get broker auth error! {} {}", orgId, e.getMessage());
                return null;
            }
        } else {
            return BROKER_AUTH_CACHE.getIfPresent(orgId);
        }
    }

    /**
     * broker认证缓存
     */
    @Data
    @Builder
    public static class BrokerApiAuth {
        private String apiKey;
        /**
         * 刷新时间（加签时间+4分钟）
         */
        private long refreshTime;
        /**
         * 认证串
         */
        private String authData;
    }
}
