package io.bhex.broker.quote.util;

public class KeyUtils {

    public static String getSymbolNameKey(Long orgId, String symbolName) {
        return "SN:" + orgId + ":" + symbolName;
    }

    public static String getSymbolIdKey(Long orgId, String symbolId) {
        return "SN:" + orgId + ":" + symbolId;
    }

    public static String getPositionKey(Long exchangeId, String symbolId) {
        return "SN:" + exchangeId + ":" + symbolId;
    }
}
