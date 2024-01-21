package io.bhex.broker.quote.metrics;

import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PushMetrics {

    private static final String EXCHANGE = "exchange";
    private static final String TOPIC = "topic";
    private static final String SYMBOL = "symbol";

    /**
     * buckets
     */
    private static final double[] LATENCY_BUCKETS = new double[]{
        0D, 1D, 5D, 10D, 20D, 50D, 100D, 400D, 500D, 1000D, 5000D, 10000D
    };

    private static final Gauge GRPC_CHANNEL_GAUGE = Gauge.build()
        .name("quote_server_grpc_channel_to_quote_engine")
        .help("The grpc channel number of quote_server and quote_engine.")
        .register();

    private static final Gauge QUOTE_BEAN_COUNTER = Gauge.build()
        .name("quote_server_spring_bean")
        .help("Quote relative bean number.")
        .labelNames(EXCHANGE, SYMBOL)
        .register();

    private static final Histogram RECEIVED_FROM_QUOTE_ENGINE_LATENCY = Histogram.build()
        .buckets(LATENCY_BUCKETS)
        .name("quote_server_received_event_latency")
        .help("Message received from quote-engine latency in ms")
        .labelNames(EXCHANGE, SYMBOL, TOPIC)
        .register();

    private static final Histogram RECEIVED_FROM_MATCH_ENGINE_LATENCY = Histogram.build()
        .buckets(LATENCY_BUCKETS)
        .name("quote_push_received_from_match_latency")
        .help("Message received from match latency in ms")
        .labelNames(EXCHANGE, TOPIC)
        .register();

    private static final Histogram PUSHED_FROM_MATCH_LATENCY = Histogram.build()
        .buckets(LATENCY_BUCKETS)
        .name("quote_push_pushed_from_match_latency")
        .help("Message pushed to client latency in ms.")
        .labelNames(EXCHANGE, TOPIC)
        .register();

    private static final Histogram PUSHED_FROM_MATCH_LATENCY_V2 = Histogram.build()
        .buckets(LATENCY_BUCKETS)
        .name("quote_push_pushed_from_match_latency_v2")
        .help("Message pushed to client latency in ms.")
        .labelNames(TOPIC)
        .register();


    private static final Gauge KLINE_LAST_TIME_HISTOGRAM = Gauge.build()
        .name("quote_last_kline_time")
        .help("Last m1 kline time.")
        .labelNames(EXCHANGE, TOPIC)
        .register();

    private static final Gauge CLIENT_GAUGE = Gauge.build()
        .name("quote_push_topic_symbol_gauge")
        .help("The number of topics_symbols ")
        .labelNames(TOPIC)
        .register();

    private static final Gauge CHANNEL_GAUGE = Gauge.build()
        .name("quote_push_channel_gauge")
        .help("The number of channels.")
        .register();

    public static void markM1KlineLastTime(Long exchangeId, String symbol, long time) {
        KLINE_LAST_TIME_HISTOGRAM
            .labels(String.valueOf(exchangeId), symbol)
            .set(time);
    }

    public static void recordQuoteEngineEvent(Long ex, String symbol, String tp, long t) {
        RECEIVED_FROM_QUOTE_ENGINE_LATENCY
            .labels(String.valueOf(ex), symbol, tp)
            .observe(t - System.currentTimeMillis());
    }

    public static void receivedMessageFromGRpc(Long exchangeId, String topic, long latency) {
        RECEIVED_FROM_MATCH_ENGINE_LATENCY
            .labels(String.valueOf(exchangeId), topic)
            .observe(latency);
    }

    public static void pushedMessageLatency(Long exchangeId, String topic, long latency) {
        PUSHED_FROM_MATCH_LATENCY
            .labels(String.valueOf(exchangeId), topic)
            .observe(latency);
    }

    public static void pushedMessageLatency(String topic, long latency) {
        pushedMessageLatency(0L, topic, latency);
    }

    public static void pushedMessageLatencyV2(String topic, long latency) {
        PUSHED_FROM_MATCH_LATENCY_V2
            .labels(topic)
            .observe(latency);
    }

    public static void addSubscriber(String topic) {
        CLIENT_GAUGE.labels(topic)
            .inc();
    }

    public static void removeSubscriber(String topic) {
        CLIENT_GAUGE.labels(topic)
            .dec();
    }

    public static void addQuoteEngineGRpcChannel() {
        GRPC_CHANNEL_GAUGE
            .inc();
    }

    public static void removeQuoteEngineGRpcChannel() {
        GRPC_CHANNEL_GAUGE
            .dec();
    }

    public static void addQuoteSpringBean(Long exchangeId, String symbol) {
        QUOTE_BEAN_COUNTER
            .labels(String.valueOf(exchangeId), symbol)
            .inc();
    }

    public static void removeQuoteSpringBean(Long exchangeId, String symbol) {
        QUOTE_BEAN_COUNTER
            .labels(String.valueOf(exchangeId), symbol)
            .dec();
    }

    public static void addChannel() {
        CHANNEL_GAUGE.inc();
    }

    public static void removeChannel() {
        CHANNEL_GAUGE.dec();
    }
}
