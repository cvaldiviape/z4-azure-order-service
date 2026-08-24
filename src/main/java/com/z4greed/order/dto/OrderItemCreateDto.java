package com.z4greed.order.dto;

import com.z4greed.order.entity.OrderEntity;
import java.math.BigDecimal;
import lombok.Builder;

@Builder
public record OrderItemCreateDto(
    OrderEntity order,
    Long productId,
    String productName,
    BigDecimal unitPrice,
    Integer quantity,
    BigDecimal subtotal) {}
