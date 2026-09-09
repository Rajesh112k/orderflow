package orderflow.dto;

import lombok.Builder;
import lombok.Getter;

import java.math.BigDecimal;
@Getter
@Builder
public class ProductResponse {

    private Long id;
    private String name;
    private BigDecimal price;
    private Integer quantity;
}