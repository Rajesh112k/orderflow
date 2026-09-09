package orderflow.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
@Getter
@Setter
public class CreateProductRequest {


        @NotBlank(message = "Product name is required")
        private String name;

        @NotNull(message = "Price is required")
        @Positive(message = "Price must be greater than zero")
        private BigDecimal price;

        @NotNull(message = "Quantity is required")
        @PositiveOrZero(message = "Quantity cannot be negative")
        private Integer quantity;

}