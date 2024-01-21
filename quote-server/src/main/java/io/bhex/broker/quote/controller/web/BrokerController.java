package io.bhex.broker.quote.controller.web;

import io.bhex.broker.quote.common.Result;
import io.bhex.broker.quote.enums.RealtimeIntervalEnum;
import io.bhex.broker.quote.exception.BizException;
import io.bhex.broker.quote.listener.TopNListener;
import io.bhex.broker.quote.service.web.QuoteService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

import static io.bhex.broker.quote.enums.StringsConstants.DEFAULT_TOP_N;

@RestController
@RequestMapping("${bhex.inner-api.prefix}/broker")
public class BrokerController {
    private final QuoteService quoteService;

    @Autowired
    public BrokerController(QuoteService quoteService) {
        this.quoteService = quoteService;
    }

    @GetMapping("/top")
    public Result topN(@RequestParam(defaultValue = StringUtils.EMPTY + DEFAULT_TOP_N) int limit,
                       @RequestParam(defaultValue = TopNListener.ALL + "", required = false) int type,
                       @RequestParam(required = false) String realtimeInterval,
                       HttpServletRequest request)
        throws BizException {
        String host = request.getHeader(HttpHeaders.HOST);
        RealtimeIntervalEnum realtimeIntervalEnum = RealtimeIntervalEnum.intervalOf(realtimeInterval);
        return Result.webSuccess(quoteService.getTopN(host, type, limit, realtimeIntervalEnum));
    }

    @GetMapping("/tickers")
    public Result tickers(@RequestParam(defaultValue = TopNListener.ALL + "", required = false) int type,
                          @RequestParam(required = false) String realtimeInterval,
                          HttpServletRequest request) throws BizException {
        String host = request.getHeader(HttpHeaders.HOST);
        RealtimeIntervalEnum realtimeIntervalEnum = RealtimeIntervalEnum.intervalOf(realtimeInterval);
        return Result.webSuccess(quoteService.getBrokerTickers(host, type, realtimeIntervalEnum));
    }
}
