package io.bhex.broker.quote.common;

/**
 * 1m
 * 3m
 * 5m
 * 15m
 * 30m
 * 1h
 * 2h
 * 4h
 * 6h
 * 8h
 * 12h
 * 1d
 * 3d
 * 1w
 * 1M
 */
public enum KlineTypes {
    min1("1m", 60 * 1000L),
    min3("3m", 3 * 60 * 1000L),
    min5("5m", 5 * 60 * 1000L),
    min15("15m", 15 * 60 * 1000L),
    min30("30m", 30 * 60 * 1000L),
    hour("1h", 60 * 60 * 1000L),
    hour2("2h", 2 * 60 * 60 * 1000L),
    hour4("4h", 4 * 60 * 60 * 1000L),
    hour6("6h", 6 * 60 * 60 * 1000L),
    hour8("8h", 8 * 60 * 60 * 1000L),
    hour12("12h", 12 * 60 * 60 * 1000L),
    day("1d", 24 * 60 * 60 * 1000L),
    day_8("1d+8", 24 * 60 * 60 * 1000L),
    week("1w", 7 * 24 * 60 * 60 * 1000L),
    week_8("1w+8", 7 * 24 * 60 * 60 * 1000L),
    month("1M", 30 * 7 * 24 * 60 * 60 * 1000L),
    month_8("1M+8", 30 * 7 * 24 * 60 * 60 * 1000L);
    String interval;
    long unitMillis;

    KlineTypes(String interval, long unitMillis) {
        this.interval = interval;
        this.unitMillis = unitMillis;
    }

    public String getInterval() {
        return interval;
    }

    public long getUnitMillis() {
        return unitMillis;
    }

    public static KlineTypes getType(String type) {
        for (KlineTypes klineTypes : values()) {
            if (klineTypes.getInterval().equals(type)) {
                return klineTypes;
            }
        }
        return null;
    }
}
