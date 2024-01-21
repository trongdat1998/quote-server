package io.bhex.broker.quote.enums;

/**
 * @author wangsc
 * @description 升级Interval
 * @date 2020-08-08 15:55
 */
public enum UpdateIntervalEnum {
    /**
     * 支持更新的周期
     */
    D1("1d", "1d+8"),
    W1("1w", "1w+8"),
    MON1("1M", "1M+8");
    private final String interval;
    private final String updateInterval;

    UpdateIntervalEnum(String interval, String updateInterval) {
        this.interval = interval;
        this.updateInterval = updateInterval;
    }

    public static UpdateIntervalEnum intervalOf(String interval) {
        UpdateIntervalEnum[] enums = values();
        for (UpdateIntervalEnum intervalEnum : enums) {
            if (intervalEnum.interval.equals(interval)) {
                return intervalEnum;
            }
        }
        return null;
    }

    public String getInterval() {
        return interval;
    }

    public String getUpdateInterval() {
        return updateInterval;
    }
}
