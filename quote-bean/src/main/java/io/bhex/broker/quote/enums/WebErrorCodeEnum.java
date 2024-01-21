package io.bhex.broker.quote.enums;

public enum WebErrorCodeEnum {

    // 100,000 ~ 200,000
    SUCCESS(200, "SUCCESS"),
    KLINE_TYPE_ERROR(-100000, "Invalid Kline interval!"),
    EXCHANGE_ID_ERROR(-100001, "Invalid exchangeId!"),
    SYMBOL_NOT_SUPPORT(-100002, "Invalid symbol!"),
    SYMBOL_REQUIRED(-100003, "Symbol required!"),
    NO_SUCH_BROKER(-100004, "No such broker!"),
    SYSTEM_ERROR(-9999, "Server Error");
    public int code;
    public String message;
    WebErrorCodeEnum(int code, String message) {
        this.code = code;
        this.message = message;
    }
}
