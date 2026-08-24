package com.z4greed.order.dto;

import com.z4greed.order.enums.OrderStatusEnum;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import lombok.Builder;

@Builder
public record OrderCreateDto(
    Long customerId,
    OrderStatusEnum status,
    BigDecimal totalAmount,
    String currency,
    String correlationId,
    String paymentToken,
    LocalDateTime createdAt) {}
