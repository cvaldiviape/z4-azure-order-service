package com.z4greed.order.dto;

import jakarta.validation.constraints.*;
import java.math.BigDecimal;
import lombok.Builder;

@Builder
public record ItemRequestDto(
    @NotNull Long productId,
    @NotBlank String productName,
    @DecimalMin("0.01") BigDecimal unitPrice,
    @Min(1) int quantity) {}
