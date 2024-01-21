package io.bhex.broker.quote.mq;

public class Groups {
    private static final String QUOTE_SERVER_CONSUMER_GROUP = "quote_server_consumer_grp";

    public static String getMarkPriceConsumerGroupName() {
        return QUOTE_SERVER_CONSUMER_GROUP;
    }
}
