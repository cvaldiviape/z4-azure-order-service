package com.z4greed.order.dto;

import java.math.BigDecimal;
import java.util.List;
import lombok.Builder;

@Builder
public record OrderResponseDto(
    Long id,
    Long customerId,
    String status,
    BigDecimal totalAmount,
    String currency,
    String correlationId,
    List<ItemResponseDto> listItems) {}
