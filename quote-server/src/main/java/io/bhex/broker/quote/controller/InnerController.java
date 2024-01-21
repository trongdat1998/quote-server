package io.bhex.broker.quote.controller;

import io.bhex.broker.grpc.basic.Symbol;
import io.bhex.broker.grpc.broker.Broker;
import io.bhex.broker.quote.client.QuoteInitializeManager;
import io.bhex.broker.quote.common.Result;
import io.bhex.broker.quote.repository.SymbolRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/tool")
public class InnerController {

    private final SymbolRepository symbolRepository;
    private final QuoteInitializeManager quoteInitializeManager;

    @Autowired
    public InnerController(SymbolRepository symbolRepository, QuoteInitializeManager quoteInitializeManager) {
        this.symbolRepository = symbolRepository;
        this.quoteInitializeManager = quoteInitializeManager;
    }

    @GetMapping("/symbol/node/info")
    public Result nodeInfo(@RequestParam Long exchangeId, @RequestParam String symbol) {
        return Result.webSuccess(quoteInitializeManager.getNodeInfo(exchangeId, symbol));
    }

    @GetMapping("/symbol/all")
    public Result allExchangeIdSymbol() {
        return Result.webSuccess(symbolRepository.getAllExchangeIdSymbolMap());
    }

    @GetMapping("/symbol/option")
    public Result optionExchangeIdSymbol() {
        return Result.webSuccess(symbolRepository.getOptionExchangeIdSymbolsMap());
    }

    @GetMapping("/symbol/futures")
    public Result futuresExchangeIdSymbol() {
        return Result.webSuccess(symbolRepository.getFuturesExchangeIdSymbolsMap());
    }

    @GetMapping("/symbol/other")
    public Result otherExchangeIdSymbol() {
        return Result.webSuccess(symbolRepository.getOtherExchangeIdSymbolsMap());
    }

    @GetMapping("/symbol/merged")
    public Result symbolsMergedListMap() {
        return Result.webSuccess(symbolRepository.getSymbolsMergedListMap());
    }

    @GetMapping("/symbol/index")
    public Result symbolIndex() {
        return Result.webSuccess(symbolRepository.getSymbolIndicesMap());
    }

    @GetMapping("/symbol/shared")
    public Result symbolShared() {
        return Result.webSuccess(symbolRepository.getSharedExchangeMap());
    }

    @GetMapping("/symbol/reversed/shared")
    public Result reversedShared() {
        return Result.webSuccess(symbolRepository.getReversedSharedExchangeMap());
    }

    @GetMapping("/symbol/broker/info")
    public Result symbolBrokerInfo() {
        return Result.webSuccess(symbolRepository.getExchangeIdSymbolOrgIdSetMap());
    }

    @GetMapping("/subscription")
    public Result subscription() {
        return Result.webSuccess(quoteInitializeManager.getSubscriptionStatusMap());
    }

    @GetMapping("/symbol/broker/symbols")
    public Result brokerSymbols() {
        Map<String, Symbol> symbolMap = symbolRepository.getBrokerSymbolMap();
        Map<String, String> resultMap = new HashMap<>();
        for (Map.Entry<String, Symbol> stringSymbolEntry : symbolMap.entrySet()) {
            resultMap.put(stringSymbolEntry.getKey(), stringSymbolEntry.getValue().toString());
        }
        return Result.webSuccess(resultMap);
    }

    @GetMapping("/brokers")
    public Result brokers() {
        Set<Broker> brokerSet = symbolRepository.getBrokerSet();
        List<String> brokerStringList = brokerSet.stream()
            .map(Broker::toString)
            .collect(Collectors.toList());
        return Result.webSuccess(brokerStringList);
    }

}
