package com.z4greed.order.dto;

import java.math.BigDecimal;
import lombok.Builder;

@Builder
public record ItemResponseDto(
    Long productId,
    String productName,
    BigDecimal unitPrice,
    Integer quantity,
    BigDecimal subtotal) {}
