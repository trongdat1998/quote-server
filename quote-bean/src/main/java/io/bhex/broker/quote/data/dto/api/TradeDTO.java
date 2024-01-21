package io.bhex.broker.quote.data.dto.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.bhex.base.match.Ticket;
import io.bhex.broker.quote.data.dto.IEngineData;
import io.bhex.broker.quote.data.dto.TicketDTO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TradeDTO implements IEngineData {

    @JsonIgnore
    public static final int MAX_LIMIT = 1000;
    @JsonIgnore
    public static final int DEFAULT_LIMIT = 500;
    /**
     * 时间
     */
    @JsonProperty("time")
    private long time;

    /**
     * 成交价格
     */
    private BigDecimal price;

    /**
     * 成交数理
     */
    @JsonProperty("qty")
    private BigDecimal quantity;

    /**
     *
     */
    @JsonProperty("isBuyerMaker")
    private boolean maker;

    public static TradeDTO parse(Ticket ticket) {
        return TradeDTO.builder()
            .time(ticket.getTime())
            .maker(!ticket.getIsBuyerMaker())
            .price(new BigDecimal(ticket.getPrice().getStr()))
            .quantity(new BigDecimal(ticket.getQuantity().getStr()))
            .build();
    }

    public static TradeDTO parse(TicketDTO ticketDTO) {
        return TradeDTO.builder()
            .time(ticketDTO.getTime())
            .maker(ticketDTO.isMakerBuy())
            .price(new BigDecimal(ticketDTO.getPrice()))
            .quantity(new BigDecimal(ticketDTO.getQuantity()))
            .build();
    }
}
