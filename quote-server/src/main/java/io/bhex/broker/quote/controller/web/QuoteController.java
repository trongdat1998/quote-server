package io.bhex.broker.quote.controller.web;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.bhex.broker.quote.common.KlineTypes;
import io.bhex.broker.quote.common.Result;
import io.bhex.broker.quote.data.Symbols;
import io.bhex.broker.quote.data.dto.DepthDTO;
import io.bhex.broker.quote.data.dto.KlineItemDTO;
import io.bhex.broker.quote.data.dto.MarkPriceDTO;
import io.bhex.broker.quote.data.dto.RealTimeDTO;
import io.bhex.broker.quote.data.dto.TicketDTO;
import io.bhex.broker.quote.enums.ErrorCodeEnum;
import io.bhex.broker.quote.enums.RealtimeIntervalEnum;
import io.bhex.broker.quote.exception.BizException;
import io.bhex.broker.quote.job.PlatformTask;
import io.bhex.broker.quote.service.web.QuoteService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static io.bhex.broker.quote.common.SymbolConstants.COMMA;

@RestController
@RequestMapping("${bhex.inner-api.prefix}")
@Slf4j
public class QuoteController {

    private final QuoteService quoteService;
    private final PlatformTask platformTask;

    @Autowired
    public QuoteController(QuoteService quoteService, PlatformTask platformTask) {
        this.quoteService = quoteService;
        this.platformTask = platformTask;
    }

    @GetMapping("/markPrice")
    public Result<List<MarkPriceDTO>> markPrice(String symbol) {
        if (StringUtils.isEmpty(symbol)) {
            return Result.webSuccess(Collections.emptyList());
        }
        Set<Symbols> symbolsSet = Symbols.parseToSet(symbol);
        return Result.webSuccess(quoteService.getMarkPriceDTOList(symbolsSet));
    }

    @GetMapping("/history/klines")
    public Result<List<KlineItemDTO>> historyKline(String symbol,
                                                   String interval,
                                                   @RequestParam(defaultValue = "" + KlineItemDTO.MAX_LIMIT) int limit,
                                                   @RequestParam(required = false) Long to,
                                                   HttpServletRequest request)
        throws BizException, ExecutionException {
        List<Symbols> symbols = Symbols.parse(symbol);
        // At most 1,000 items
        if (limit < 1 || limit > KlineItemDTO.MAX_LIMIT) {
            limit = KlineItemDTO.MAX_LIMIT;
        }

        if (StringUtils.isEmpty(symbol)) {
            throw new BizException(ErrorCodeEnum.SYMBOL_REQUIRED);
        }

        // kline type check
        KlineTypes klineType = KlineTypes.getType(interval);
        if (Objects.isNull(klineType)) {
            throw new BizException(ErrorCodeEnum.PERIOD_EMPTY);
        }

        String host = request.getHeader(HttpHeaders.HOST);
        List<KlineItemDTO> klineItemDTOList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(symbols)) {
            Symbols klineSymbols = symbols.get(0);
            if (Objects.isNull(to) || to > System.currentTimeMillis()) {
                to = System.currentTimeMillis();
            }
            long from = to - limit * klineType.getUnitMillis();
            klineItemDTOList = quoteService.getHistoryKline(host, klineSymbols.getExchangeId(),
                    klineSymbols.getSymbol(), klineType, from, to);
        }
        return Result.webSuccess(klineItemDTOList);
    }

    /**
     * 返回同一个交易所的多个币对k线，
     */
    @GetMapping("/multi/kline")
    public Result<Map<String, List<KlineItemDTO>>> multiKline(
        Long exchangeId, String symbol, String interval,
        @RequestParam(defaultValue = "" + KlineItemDTO.MAX_MULTI_LIMIT) int limit,
        HttpServletRequest request)
        throws BizException {
        List<Symbols> symbols = Symbols.parse(exchangeId, symbol);
        if (limit <= 0) {
            return Result.webSuccess(Maps.newHashMap());
        }
        if (symbols.size() > 20) {
            symbols = symbols.subList(0, 20);
        }
        limit = Math.min(limit, KlineItemDTO.MAX_MULTI_LIMIT);
        KlineTypes klineType = KlineTypes.getType(interval);
        if (Objects.isNull(klineType)) {
            throw new BizException(ErrorCodeEnum.PERIOD_ERROR);
        }
        String host = request.getHeader(HttpHeaders.HOST);
        Map<String, List<KlineItemDTO>> klineItemDTOListMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(symbols)) {
            for (Symbols klineSymbols : symbols) {
                try {
                    List<KlineItemDTO> klineItemDTOList = quoteService.getLatestMemKline(host, klineSymbols.getExchangeId(),
                            klineSymbols.getSymbol(), klineType, limit);
                    klineItemDTOListMap.put(klineSymbols.getSymbol(), klineItemDTOList);
                } catch (Exception e) {
                    log.warn(e.getMessage(), e);
                }
            }
        }
        return Result.webSuccess(klineItemDTOListMap);
    }

    @GetMapping("/index/klines")
    public Result<List<KlineItemDTO>> indexKline(@RequestParam String symbol,
                                                 @RequestParam String interval,
                                                 @RequestParam(defaultValue = "" + KlineItemDTO.MAX_LIMIT) int limit,
                                                 @RequestParam(required = false) Long to) throws BizException {
        // At most 1,000 items
        if (limit < 1 || limit > KlineItemDTO.MAX_LIMIT) {
            limit = KlineItemDTO.MAX_LIMIT;
        }

        // kline type check
        KlineTypes klineType = KlineTypes.getType(interval);
        if (Objects.isNull(klineType)) {
            throw new BizException(ErrorCodeEnum.PERIOD_EMPTY);
        }

        List<KlineItemDTO> klineItemDTOList;
        if (Objects.isNull(to)) {
            klineItemDTOList = quoteService.getLatestMemIndexKline(symbol, klineType, limit);
        } else {
            if (to > System.currentTimeMillis()) {
                to = System.currentTimeMillis();
            }
            long from = to - limit * klineType.getUnitMillis();
            klineItemDTOList = quoteService.getMemIndexKline(symbol, klineType, from, to);
        }
        return Result.webSuccess(klineItemDTOList);

    }

    /**
     * K线
     */
    @GetMapping("/klines")
    public Result<List<KlineItemDTO>> kline(String symbol,
                                            String interval,
                                            @RequestParam(defaultValue = "" + KlineItemDTO.MAX_LIMIT) int limit,
                                            @RequestParam(required = false) Long to,
                                            HttpServletRequest request)
        throws BizException {
        List<Symbols> symbols = Symbols.parse(symbol);
        // At most 1,000 items
        if (limit < 1 || limit > KlineItemDTO.MAX_LIMIT) {
            limit = KlineItemDTO.MAX_LIMIT;
        }
        // kline type check
        KlineTypes klineType = KlineTypes.getType(interval);
        if (Objects.isNull(klineType)) {
            throw new BizException(ErrorCodeEnum.PERIOD_EMPTY);
        }
        String host = request.getHeader(HttpHeaders.HOST);
        List<KlineItemDTO> klineItemDTOList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(symbols)) {
            Symbols klineSymbols = symbols.get(0);
            if (Objects.isNull(to)) {
                klineItemDTOList = quoteService.getLatestMemKline(host, klineSymbols.getExchangeId(),
                        klineSymbols.getSymbol(), klineType, limit);
            } else {
                if (to > System.currentTimeMillis()) {
                    to = System.currentTimeMillis();
                }
                long from = to - limit * klineType.getUnitMillis();
                klineItemDTOList = quoteService.getMemKline(host, klineSymbols.getExchangeId(),
                        klineSymbols.getSymbol(), klineType, from, to);
            }
        }
        return Result.webSuccess(klineItemDTOList);
    }

    /**
     * 深度
     */
    @GetMapping("/depth")
    public Result<List<DepthDTO>> depth(String symbol,
                                        @RequestParam(required = false) Integer dumpScale,
                                        @RequestParam(defaultValue = "" + DepthDTO.MAX_LIMIT) int limit)
        throws BizException {
        List<Symbols> symbols = Symbols.parse(symbol);
        List<DepthDTO> depthDTOList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(symbols)) {
            for (Symbols depthSymbols : symbols) {
                DepthDTO depth;
                if (Objects.isNull(dumpScale)) {
                    depth = quoteService.getDepth(depthSymbols.getExchangeId(), depthSymbols.getSymbol());
                } else {
                    depth = quoteService.getDepth(depthSymbols.getExchangeId(), depthSymbols.getSymbol(), dumpScale);
                }
                depth.truncate(limit);
                depthDTOList.add(depth);
            }
        }
        return Result.webSuccess(depthDTOList);
    }

    /**
     * 最新成交
     */
    @GetMapping("/trades")
    public Result<List<TicketDTO>> trade(String symbol,
                                         @RequestParam(defaultValue = "" + TicketDTO.MAX_LIMIT) int limit)
        throws BizException {
        List<Symbols> symbols = Symbols.parse(symbol);
        if (limit < 1 || limit > TicketDTO.MAX_LIMIT) {
            limit = TicketDTO.MAX_LIMIT;
        }

        if (CollectionUtils.isEmpty(symbols)) {
            throw new BizException(ErrorCodeEnum.SYMBOL_REQUIRED);
        }

        // 成交记录只会取第一个，client的需求
        Symbols tradeSymbols = symbols.get(0);

        List<TicketDTO> ticketList = quoteService.getTrades(tradeSymbols.getExchangeId(), tradeSymbols.getSymbol());
        if (ticketList.size() > limit) {
            ticketList = ticketList.subList(ticketList.size() - limit, ticketList.size());
        }

        return Result.webSuccess(ticketList);
    }

    /**
     * 最新行情
     */
    @RequestMapping("/ticker")
    public Result<List<RealTimeDTO>> realTime(String symbol,
                                              @RequestParam(required = false) String realtimeInterval) throws BizException {
        List<RealTimeDTO> resultList = new ArrayList<>();
        if (StringUtils.isEmpty(symbol)) {
            throw new BizException(ErrorCodeEnum.SYMBOL_REQUIRED);
        }
        RealtimeIntervalEnum realtimeIntervalEnum = RealtimeIntervalEnum.intervalOf(realtimeInterval);
        List<Symbols> symbolsList = Symbols.parse(symbol);
        if (CollectionUtils.isNotEmpty(symbolsList)) {
            for (Symbols symbols : symbolsList) {
                resultList.add(quoteService.getRealTime(symbols.getExchangeId(), symbols.getSymbol(), realtimeIntervalEnum));
            }
            return Result.webSuccess(resultList);
        }
        throw new BizException(ErrorCodeEnum.SYMBOLS_NOT_SUPPORT);
    }

    @GetMapping("/indices")
    public Result<Map<String, BigDecimal>> indices(String symbol) throws BizException {
        if (StringUtils.isEmpty(symbol)) {
            throw new BizException(ErrorCodeEnum.SYMBOL_REQUIRED);
        }

        String[] symbols = StringUtils.split(symbol, COMMA);
        Set<String> symbolSet = Sets.newHashSet(symbols);
        return Result.webSuccess(platformTask.getIndexBySymbolSet(symbolSet));
    }

    @GetMapping("/obj/indices")
    public Result objIndices(String symbol) throws BizException {
        if (StringUtils.isEmpty(symbol)) {
            throw new BizException(ErrorCodeEnum.SYMBOL_REQUIRED);
        }

        String[] symbols = StringUtils.split(symbol, COMMA);
        Set<String> symbolSet = Sets.newHashSet(symbols);
        return Result.webSuccess(platformTask.getIndexDTOMapBySymbolSet(symbolSet));
    }


}
