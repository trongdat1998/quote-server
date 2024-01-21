package io.bhex.broker.quote.controller.api;

import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.Index;
import io.bhex.base.quote.KLine;
import io.bhex.broker.quote.common.KlineTypes;
import io.bhex.broker.quote.data.dto.MarkPriceDTO;
import io.bhex.broker.quote.data.dto.RealTimeDTO;
import io.bhex.broker.quote.data.dto.TicketDTO;
import io.bhex.broker.quote.data.dto.api.DepthDTO;
import io.bhex.broker.quote.data.dto.api.IndexDTO;
import io.bhex.broker.quote.data.dto.api.KlineDTO;
import io.bhex.broker.quote.data.dto.api.TickerDTO;
import io.bhex.broker.quote.data.dto.api.TickerPriceDTO;
import io.bhex.broker.quote.data.dto.api.TradeDTO;
import io.bhex.broker.quote.enums.ErrorCodeEnum;
import io.bhex.broker.quote.enums.RealtimeIntervalEnum;
import io.bhex.broker.quote.exception.BizException;
import io.bhex.broker.quote.service.web.QuoteService;
import lombok.extern.slf4j.Slf4j;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static io.bhex.broker.quote.enums.StringsConstants.CONTRACT;
import static io.bhex.broker.quote.enums.StringsConstants.OPTION;
import static io.netty.util.internal.StringUtil.COMMA;

@RestController
@RequestMapping({"${bhex.api.prefix}", "${bhex.api.prefix}/" + OPTION, "${bhex.api.prefix}/" + CONTRACT})
@Slf4j
public class ApiQuoteController {

    private final QuoteService quoteService;

    @Autowired
    public ApiQuoteController(QuoteService quoteService) {
        this.quoteService = quoteService;
    }

    /**
     * 标记价格
     */
    @GetMapping("/markPrice")
    public MarkPriceDTO markPrice(@RequestParam String symbol,
                                  HttpServletRequest request) throws BizException {
        String host = request.getHeader(HttpHeaders.HOST);
        return quoteService.getMarkPrice(host, symbol);
    }

    /**
     * 深度
     */
    @GetMapping("/depth/merged")
    public DepthDTO depth(@RequestParam String symbol,
                          @RequestParam(required = false) Integer scale,
                          @RequestParam(defaultValue = "" + DepthDTO.MAX_LIMIT) int limit,
                          HttpServletRequest request)
        throws BizException {
        String host = request.getHeader(HttpHeaders.HOST);
        DepthDTO depth = DepthDTO.parse(quoteService.getDepth(host, symbol, scale));
        depth.truncate(limit);
        return depth;
    }

    /**
     * 深度
     *
     * @param limit 200
     */
    @GetMapping("/depth")
    public DepthDTO depth(@RequestParam String symbol,
                          @RequestParam(defaultValue = "" + DepthDTO.MAX_LIMIT) int limit,
                          HttpServletRequest request) throws BizException {
        String host = request.getHeader(HttpHeaders.HOST);
        DepthDTO depthDTO = DepthDTO.parse(quoteService.getDepth(host, symbol));
        depthDTO.setSymbol(symbol);
        depthDTO.truncate(limit);
        return depthDTO;
    }

    /**
     * K线
     */
    @GetMapping("/klines")
    public List<KlineDTO> kline(@RequestParam String symbol,
                                @RequestParam String interval,
                                @RequestParam(required = false) Long startTime,
                                @RequestParam(required = false) Long endTime,
                                @RequestParam(defaultValue = "" + KlineDTO.MAX_LIMIT) int limit,
                                HttpServletRequest request)
        throws BizException {
        String host = request.getHeader(HttpHeaders.HOST);
        // At most 1,000 items
        if (limit < 1 || limit > KlineDTO.MAX_LIMIT) {
            limit = KlineDTO.MAX_LIMIT;
        }
        // kline type check
        KlineTypes klineType = KlineTypes.getType(interval);
        if (Objects.isNull(klineType)) {
            throw new BizException(ErrorCodeEnum.PERIOD_ERROR);
        }
        if (Objects.isNull(endTime)) {
            endTime = System.currentTimeMillis();
            startTime = endTime - limit * klineType.getUnitMillis();
        } else {
            if (endTime > System.currentTimeMillis()) {
                endTime = System.currentTimeMillis();
            }
            if (Objects.isNull(startTime)) {
                startTime = endTime - limit * klineType.getUnitMillis();
            } else {
                startTime = Math.max(startTime, endTime - limit * klineType.getUnitMillis());
            }
            if (startTime > endTime) {
                startTime = endTime;
            }
        }

        List<KLine> kLineList = quoteService.getOriginMemKline(host, symbol, klineType, startTime, endTime);
        if (kLineList.size() > limit) {
            kLineList = kLineList.subList(kLineList.size() - limit, kLineList.size());
        }

        List<KlineDTO> klineDTOList = new ArrayList<>();
        for (KLine kLine : kLineList) {
            klineDTOList.add(KlineDTO.parse(kLine));
        }

        return klineDTOList;
    }

    /**
     * 最新成交
     */
    @GetMapping("/trades")
    public List<TradeDTO> trade(@RequestParam String symbol,
                                @RequestParam(defaultValue = "" + TradeDTO.DEFAULT_LIMIT) int limit,
                                HttpServletRequest request)
        throws BizException {
        String host = request.getHeader(HttpHeaders.HOST);
        if (limit < 1 || limit > TradeDTO.MAX_LIMIT) {
            limit = TradeDTO.MAX_LIMIT;
        }

        List<TicketDTO> ticketList = quoteService.getTrades(host, symbol);
        if (ticketList.size() > limit) {
            ticketList = ticketList.subList(ticketList.size() - limit, ticketList.size());
        }
        List<TradeDTO> tradeList = new ArrayList<>();
        for (TicketDTO ticket : ticketList) {
            tradeList.add(TradeDTO.parse(ticket));
        }

        return tradeList;
    }

    /**
     * 最新行情
     */
    @GetMapping("/ticker/24hr")
    public Object realTime(@RequestParam(required = false) String symbol,
                           @RequestParam(required = false) String realtimeInterval,
                           HttpServletRequest request)
        throws BizException {
        String host = request.getHeader(HttpHeaders.HOST);
        RealtimeIntervalEnum realtimeIntervalEnum = RealtimeIntervalEnum.intervalOf(realtimeInterval);
        if (StringUtils.isNotEmpty(symbol)) {
            return quoteService.getTicker(host, symbol, realtimeIntervalEnum);
        } else {
            String path = request.getServletPath();
            List<RealTimeDTO> realTimeList;
            if (StringUtils.containsIgnoreCase(path, OPTION)) {
                realTimeList = quoteService.getOptionRealTimeList(host, realtimeIntervalEnum);
            } else if (StringUtils.containsIgnoreCase(path, CONTRACT)) {
                realTimeList = quoteService.getFuturesRealTimeList(host, realtimeIntervalEnum);
            } else {
                realTimeList = quoteService.getRealTimeList(host, realtimeIntervalEnum);
            }
            List<TickerDTO> tickerDTOList = new ArrayList<>();
            for (RealTimeDTO realtime : realTimeList) {
                tickerDTOList.add(TickerDTO.parse(realtime));
            }
            return tickerDTOList;
        }
    }

    /**
     * 最新行情，只有价格
     */
    @GetMapping("/ticker/price")
    public Object tickerPrice(@RequestParam(required = false) String symbol,
                              @RequestParam(required = false) String realtimeInterval,
                              HttpServletRequest request)
        throws BizException {
        String host = request.getHeader(HttpHeaders.HOST);
        RealtimeIntervalEnum realtimeIntervalEnum = RealtimeIntervalEnum.intervalOf(realtimeInterval);
        if (StringUtils.isNotEmpty(symbol)) {
            return TickerPriceDTO.parse(quoteService.getRealTime(host, symbol, realtimeIntervalEnum));
        } else {
            String path = request.getServletPath();
            List<RealTimeDTO> realTimeList = null;
            if (path.contains(OPTION)) {
                realTimeList = quoteService.getOptionRealTimeList(host, realtimeIntervalEnum);
            } else if (path.contains(CONTRACT)) {
                realTimeList = quoteService.getFuturesRealTimeList(host, realtimeIntervalEnum);
            } else {
                realTimeList = quoteService.getRealTimeList(host, realtimeIntervalEnum);
            }
            List<TickerPriceDTO> tickerPriceDTOList = new ArrayList<>();
            for (RealTimeDTO realTime : realTimeList) {
                if (Objects.isNull(realTime)) {
                    continue;
                }
                tickerPriceDTOList.add(TickerPriceDTO.parse(realTime));
            }
            return tickerPriceDTOList;
        }
    }

    @GetMapping("/ticker/bookTicker")
    public Object bookTicker(@RequestParam(required = false) String symbol, HttpServletRequest request)
        throws BizException {
        String host = request.getHeader(HttpHeaders.HOST);
        if (StringUtils.isNotEmpty(symbol)) {
            return quoteService.getBookTicker(host, symbol);
        } else {
            String path = request.getServletPath();
            if (path.contains(OPTION)) {
                return quoteService.getOptionBookTickerList(host);
            } else if (path.contains(CONTRACT)) {
                return quoteService.getFuturesBookTickerList(host);
            } else {
                return quoteService.getBookTickerList(host);
            }
        }
    }

    @GetMapping("/index")
    public IndexDTO index(@RequestParam(required = false) String symbol) {
        Set<String> indexSet = new HashSet<>();
        if (StringUtils.isNotEmpty(symbol)) {
            String[] symbolArray = StringUtils.split(symbol, COMMA);
            indexSet.addAll(Arrays.asList(symbolArray));
        }
        IndexDTO.IndexDTOBuilder builder = IndexDTO.builder();
        Map<String, Index> indexMap = quoteService.getIndexMap(indexSet);
        Map<String, BigDecimal> indicesMap = new HashMap<>();
        Map<String, BigDecimal> edpMap = new HashMap<>();
        for (Map.Entry<String, Index> stringIndexEntry : indexMap.entrySet()) {
            BigDecimal index = DecimalUtil.toBigDecimal(stringIndexEntry.getValue().getIndex());
            BigDecimal edp = DecimalUtil.toBigDecimal(stringIndexEntry.getValue().getEdp());
            indicesMap.put(stringIndexEntry.getKey(), index);
            edpMap.put(stringIndexEntry.getKey(), edp);

        }
        return builder
            .index(indicesMap)
            .edp(edpMap)
            .build();
    }

}
