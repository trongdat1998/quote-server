package io.bhex.broker.quote.enums;

/**
 * @author wangsc
 * @description 涨跌幅配置开始时间
 * @date 2020-08-08 15:34
 */
public enum RealtimeIntervalEnum {
    /**
     * 24小时涨跌幅, UTC时间0点, UTC时间8点
     */
    H24("24h"),
    D1("1d"),
    D1_8("1d+8");
    private final String interval;

    RealtimeIntervalEnum(String interval) {
        this.interval = interval;
    }

    public static RealtimeIntervalEnum intervalOf(String interval) {
        RealtimeIntervalEnum[] enums = values();
        for (RealtimeIntervalEnum intervalEnum : enums) {
            if (intervalEnum.interval.equals(interval)) {
                return intervalEnum;
            }
        }
        return H24;
    }

    public String getInterval() {
        return interval;
    }
}
