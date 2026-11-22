package com.z4greed.order.dto;

import java.util.List;

// Estado actual de la Saga y todos los pasos por los que pasó, en orden.
public record PurchaseSagaDetailDto(
    PurchaseSagaSummaryDto saga,
    List<PurchaseSagaHistoryDto> timeline) {}
