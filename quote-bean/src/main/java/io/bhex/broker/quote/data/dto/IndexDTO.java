package io.bhex.broker.quote.data.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.bhex.base.proto.DecimalUtil;
import io.bhex.base.quote.Index;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.util.Base64Utils;

import java.nio.charset.StandardCharsets;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IndexDTO implements ISendData {
    private String symbol;
    private String index;
    private String edp;
    private String formula;
    private Long time;

    public static IndexDTO parse(Index indexObj) {
        return IndexDTO.builder()
            .symbol(indexObj.getSymbol())
            .index(DecimalUtil.toTrimString(indexObj.getIndex()))
            .edp(DecimalUtil.toTrimString(indexObj.getEdp()))
            .formula(new String(Base64Utils.decodeFromString(indexObj.getFormula()), StandardCharsets.UTF_8))
            .time(indexObj.getTime())
            .build();
    }

    @Override
    @JsonIgnore
    public long getCurId() {
        return 0;
    }
}
