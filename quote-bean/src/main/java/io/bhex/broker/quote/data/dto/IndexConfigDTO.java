package io.bhex.broker.quote.data.dto;

import io.bhex.base.quote.IndexConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IndexConfigDTO {
    private String name;
    private String formula;

    public static IndexConfigDTO parse(IndexConfig indexConfig) {
        return IndexConfigDTO.builder()
            .name(indexConfig.getName())
            .formula(indexConfig.getFormula())
            .build();
    }
}
