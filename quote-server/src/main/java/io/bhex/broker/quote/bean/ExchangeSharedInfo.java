package io.bhex.broker.quote.bean;

import io.bhex.broker.grpc.basic.Symbol;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ExchangeSharedInfo {

    /**
     * exchangeId -> string -> shardExchangeId
     */
    private Map<Long, Map<String, Long>> sharedExchangeMap = new HashMap<>();
    /**
     * 反向的共享关系
     * 某个交易所被共享的交易所id有哪些
     */
    private Map<Long, Map<String, Set<Long>>> reversedSharedExchangeMap = new HashMap<>();

}
