package io.bhex.broker.quote.client;

public enum QuoteEventTypeEnum {

    SUBSCRIBED,
    UNSUBSCRIBED,
    REFRESH_SUBSCRIPTION,
    MARK_ALL_UNSUBSCRIBED,
    REFRESH_ROUTER_INFO,
    RECONNECT,
    REFRESH_INDEX_SUBSCRIPTION;
}
