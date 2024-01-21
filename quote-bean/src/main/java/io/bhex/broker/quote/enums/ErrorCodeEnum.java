package io.bhex.broker.quote.enums;

public enum ErrorCodeEnum {
    SUCCESS("0", "Success"),
    OK("0", "Success"),
    WEB_SUCCESS("200", "Success"),
    CLIENT_CONNECTION_RESET("400", "Connection reset!"),
    /**
     * 错误码
     */
    SYSTEM_ERROR("-9999", "Server Error"),
    INVALID_REQUEST("-10000", "Invalid request!"),
    JSON_FORMAT_ERROR("-10001", "Invalid JSON!"),
    INVALID_EVENT("-10002", "Invalid event!"),
    REQUIRED_EVENT("-10003", "Event required!"),
    INVALID_TOPIC("-10004", "Invalid topic!"),
    REQUIRED_TOPIC("-10005", "Topic required!"),
    PARAM_EMPTY("-10007", "Params required!"),
    PERIOD_EMPTY("-10008", "Period required!"),
    PERIOD_ERROR("-10009", "Invalid period!"),
    SYMBOLS_ERROR("-100010", "Invalid Symbols!"),
    SYMBOLS_NOT_SUPPORT("-100011", "Not supported symbols"),
    BROKER_NOT_SUPPORT("-100012", "There is no such broker."),
    DUMP_SCALE_REQUIRED("-100013", "DumpScale required!"),
    EXCHANGE_ID_ERROR("-100001", "Invalid exchangeId!"),
    PARAM_TYPE_ERROR("-100002", "Param %s should be %s."),
    SYMBOL_REQUIRED("-100003", "Symbol required!"),
    TIME_OUT("-100004", "Timeout. Retry later."),
    NO_BUSINESS_WITH_EXCHANGE("-100005", "No business with any exchange."),
    RESPONSE_COMMITTED("-100006", "Client disconnected."),
    ORG_ID_REQUIRED("-100007", "OrgId required."),
    ORG_ID_INVALID("-100008", "OrgId must be a number."),
    DUMP_SCALE_ERROR("-100009", "DumpScale error."),
    INDEX_NAME_ERROR("-100010", "Index name error."),
    PARAM_ERROR("-100011", "Parameter error!"),
    PARAM_REQUIRED("-100012", "Parameter %s [%s] missing!"),
    ;
    private final String code;
    private final String desc;

    ErrorCodeEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    @Override
    public String toString() {
        return new StringBuffer("{\"code\":\"")
            .append(code)
            .append("\",\"desc\":\"")
            .append(desc)
            .append("\"}")
            .toString();
    }
}
