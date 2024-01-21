package io.bhex.broker.quote.util;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.bhex.broker.quote.common.KlineTypes;
import io.bhex.broker.quote.common.SymbolConstants;
import io.bhex.exchange.enums.KlineIntervalEnum;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

/**
 * @author wangsc
 * @description 过滤共享的历史k线查询
 * @date 2020-07-20 15:45
 */
@Slf4j
public class FilterHistoryKlineUtil {

    /**
     * 支持过滤的brokerId
     * key: brokerId + "_" + symbolId
     * value: 过滤历史限制的时间
     * 定时任务24小时不写入的过期
     */
    private final Cache<String, Long> filterHistoryMap = CacheBuilder.newBuilder()
            .expireAfterWrite(24, TimeUnit.HOURS)
            .build();

    private static volatile FilterHistoryKlineUtil filterHistoryKlineUtil;

    private FilterHistoryKlineUtil() {
    }

    public static FilterHistoryKlineUtil getInstance() {
        if (filterHistoryKlineUtil == null) {
            synchronized (FilterHistoryKlineUtil.class) {
                if (filterHistoryKlineUtil == null) {
                    filterHistoryKlineUtil = new FilterHistoryKlineUtil();
                }
            }
        }
        return filterHistoryKlineUtil;
    }

    /**
     * 获取当前币对的过滤历史k线的原始配置
     */
    private long getFilterHistoryKlineTime(Long brokerId, String symbolId) {
        Long filterTime = filterHistoryMap.getIfPresent(brokerId + SymbolConstants.UNDERLINE + symbolId);
        return filterTime != null ? filterTime : 0L;
    }

    /**
     * 获取当前币对当前interval下的过滤开始起点
     */
    public long getFilterHistoryKlineTimeStartId(Long brokerId, String symbolId, KlineIntervalEnum intervalEnum) {
        long filterTime = getFilterHistoryKlineTime(brokerId, symbolId);
        return filterTime > 0 ? KlineIntervalEnum.curTypeTime(intervalEnum, filterTime) : 0L;
    }

    /**
     * 获取当前币对当前interval下的过滤开始起点
     */
    public long getFilterHistoryKlineTimeStartId(Long brokerId, String symbolId, KlineTypes klineTypes) {
        long filterTime = getFilterHistoryKlineTime(brokerId, symbolId);
        return filterTime > 0 ? KlineIntervalEnum.curTypeTime(KlineIntervalEnum.intervalOf(klineTypes.getInterval()), filterTime) : 0L;
    }

    public void updateFilterHistoryKlineTime(Long brokerId, String symbolId, long filterTime) {
        if (brokerId != null && symbolId != null) {
            filterHistoryMap.put(brokerId + SymbolConstants.UNDERLINE + symbolId, filterTime);
        } else {
            log.warn("update Filter Kline Time Fail! brokerId={},symbolId={}", brokerId, symbolId);
        }
    }
}
