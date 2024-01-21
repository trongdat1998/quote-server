package io.bhex.broker.quote.data.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.bhex.base.match.Ticket;
import io.bhex.base.proto.DecimalUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * map.put("v", ticket.getTicketId() + "");
 * map.put("t", ticket.getTime());
 * map.put("p", DecimalUtil.toBigDecimal(ticket.getPrice()).toPlainString());
 * map.put("q", DecimalUtil.toBigDecimal(ticket.getQuantity()).toPlainString());
 * map.put("m", isBuy(ticket));
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TicketDTO implements ISendData, IEngineData {
    @JsonIgnore
    public static final int MAX_LIMIT = 100;

    @JsonProperty("v")
    private String version;

    @JsonProperty("t")
    private long time;

    @JsonProperty("p")
    private String price;

    @JsonProperty("q")
    private String quantity;

    private boolean isMakerBuy;

    public static TicketDTO parse(Ticket ticket) {
        return TicketDTO.builder()
            .version(ticket.getTicketId() + "")
            .time(ticket.getTime())
            .price(DecimalUtil.toBigDecimal(ticket.getPrice()).stripTrailingZeros().toPlainString())
            .quantity(DecimalUtil.toBigDecimal(ticket.getQuantity()).stripTrailingZeros().toPlainString())
            .isMakerBuy(isBuy(ticket))
            .build();
    }

    @JsonProperty("m")
    public boolean isMakerBuy() {
        return this.isMakerBuy;
    }

    private static boolean isBuy(Ticket ticket) {
        return !ticket.getIsBuyerMaker();
    }

    @Override
    @JsonIgnore
    public long getCurId() {
        return this.time;
    }
}
