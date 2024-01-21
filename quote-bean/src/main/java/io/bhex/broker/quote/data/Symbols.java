package io.bhex.broker.quote.data;


import io.bhex.broker.quote.enums.ErrorCodeEnum;
import io.bhex.broker.quote.exception.BizException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static ch.qos.logback.core.CoreConstants.DOT;
import static io.bhex.broker.quote.common.SymbolConstants.COMMA;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Slf4j
public class Symbols {
    /**
     * 交易所ID
     */
    private Long exchangeId;

    /**
     * 币对
     */
    private String symbol;

    public static List<Symbols> parse(String symbol) {
        List<Symbols> symbolsList = new ArrayList<>();
        if (StringUtils.isNotEmpty(symbol)) {
            String[] exchangeIdSymbolArrays = StringUtils.split(symbol, COMMA);
            for (String exchangeIdSymbolArray : exchangeIdSymbolArrays) {
                int dotIndex = StringUtils.indexOf(exchangeIdSymbolArray, DOT);
                if (dotIndex != -1) {
                    try {
                        Long exchangeId = Long.valueOf(exchangeIdSymbolArray.substring(0, dotIndex));
                        String symbolStr = exchangeIdSymbolArray.substring(dotIndex + 1);
                        symbolsList.add(Symbols.builder()
                            .exchangeId(exchangeId)
                            .symbol(symbolStr)
                            .build());
                    } catch (NumberFormatException e) {
                        log.info("ExchangeId invalid: [{}], ex: [{}]", exchangeIdSymbolArray, e.getMessage());
                    }
                }
            }
        }
        return symbolsList;
    }

    public static List<Symbols> parse(Long exchangeId, String symbols) {
        List<Symbols> symbolsList = new ArrayList<>();
        if (StringUtils.isNotEmpty(symbols)) {
            String[] subSymbols = StringUtils.split(symbols, COMMA);
            for (String subSymbol : subSymbols) {
                symbolsList.add(Symbols.builder()
                    .exchangeId(exchangeId)
                    .symbol(subSymbol)
                    .build());
            }
        }
        return symbolsList;
    }

    public static Set<Symbols> parseToSet(String symbol) {
        Set<Symbols> symbolsSet = new HashSet<>();
        if (StringUtils.isNotEmpty(symbol)) {
            String[] exchangeIdSymbolArrays = StringUtils.split(symbol, COMMA);
            for (String exchangeIdSymbolArray : exchangeIdSymbolArrays) {
                String[] exchangeIdSymbol = StringUtils.split(exchangeIdSymbolArray, DOT);
                if (exchangeIdSymbol.length >= 2) {
                    try {
                        Long exchangeId = Long.valueOf(exchangeIdSymbol[0]);
                        symbolsSet.add(Symbols.builder()
                            .exchangeId(exchangeId)
                            .symbol(exchangeIdSymbol[1])
                            .build());
                    } catch (NumberFormatException e) {
                        log.info("ExchangeId invalid: [{}], ex: [{}]", exchangeIdSymbol[0], e.getMessage());
                    }
                }
            }
        }
        return symbolsSet;
    }

    public static String toPathParams(Set<Symbols> symbolsSet) {
        if (CollectionUtils.isEmpty(symbolsSet)) {
            return StringUtils.EMPTY;
        }
        StringBuilder sb = new StringBuilder();
        for (Symbols symbols : symbolsSet) {
            sb.append(symbols.getExchangeId())
                .append(DOT)
                .append(symbols.getSymbol())
                .append(COMMA);
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public String toSymbols() {
        return this.exchangeId + (DOT + this.symbol);
    }

    /**
     * @param symbols exchangeId.symbol
     * @return Symbols
     * @throws BizException exchangeId or symbol error
     */
    public static Symbols valueOf(String symbols) throws BizException {
        if (StringUtils.isNotEmpty(symbols)) {
            int dotIndex = StringUtils.indexOf(symbols, DOT);
            if (dotIndex == -1) {
                throw new BizException(ErrorCodeEnum.SYMBOLS_NOT_SUPPORT);
            } else {
                try {
                    Long exchangeId = Long.valueOf(symbols.substring(0, dotIndex));
                    return Symbols.builder()
                        .exchangeId(exchangeId)
                        .symbol(symbols.substring(dotIndex + 1))
                        .build();
                } catch (NumberFormatException e) {
                    log.info("ExchangeId invalid: [{}], ex: [{}]", symbols, e.getMessage());
                    throw new BizException(ErrorCodeEnum.EXCHANGE_ID_ERROR);
                }
            }
        }
        throw new BizException(ErrorCodeEnum.SYMBOL_REQUIRED);
    }

}
