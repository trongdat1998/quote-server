package io.bhex.broker.quote.controller.web;

import io.bhex.broker.quote.common.Result;
import io.bhex.broker.quote.data.dto.IndexConfigDTO;
import io.bhex.broker.quote.data.dto.RateDTO;
import io.bhex.broker.quote.exception.BizException;
import io.bhex.broker.quote.service.web.QuoteService;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.bhex.broker.quote.common.SymbolConstants.COMMA;

@RestController
@RequestMapping("${bhex.inner-api.prefix}")
public class IndexController {
    private final QuoteService quoteService;

    @Autowired
    public IndexController(QuoteService quoteService) {
        this.quoteService = quoteService;
    }

    /**
     * @param request    Get exchangeId from Host
     * @param tokens     USDT,BTC
     * @param legalCoins CNY,KRW,USD
     */
    @RequestMapping("/rates")
    public Result<List<RateDTO>> rates(HttpServletRequest request,
                                       String tokens,
                                       String legalCoins) throws BizException {
        String host = request.getHeader(HttpHeaders.HOST);
        if (StringUtils.isEmpty(tokens) || StringUtils.isEmpty(legalCoins)) {
            return Result.webSuccess(Collections.emptyList());
        }
        List<String> tokenList = Arrays.asList(tokens.toUpperCase().split(COMMA));
        List<String> legalCoinList = Arrays.asList(legalCoins.toUpperCase().split(COMMA));
        List<RateDTO> rateDTOList = quoteService.getRates(host, tokenList, legalCoinList);
        return Result.webSuccess(rateDTOList);
    }

    @GetMapping("/index/config")
    public Result<IndexConfigDTO> indexFormula(@RequestParam String name) throws BizException {
        return Result.webSuccess(quoteService.getIndexConfig(name));
    }

}
