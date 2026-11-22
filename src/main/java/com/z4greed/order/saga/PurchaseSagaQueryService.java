package com.z4greed.order.saga;

import com.z4greed.shared.core.dto.PageResponseDto;
import com.z4greed.order.dto.PurchaseSagaDetailDto;
import com.z4greed.order.dto.PurchaseSagaSummaryDto;
import com.z4greed.order.enums.SagaStatusEnum;
import org.springframework.data.domain.Pageable;

// Consultas de solo lectura sobre las Sagas, para la Ops Console.
public interface PurchaseSagaQueryService {
    PageResponseDto<PurchaseSagaSummaryDto> search(SagaStatusEnum status, Pageable pageable);
    PurchaseSagaDetailDto getByOrderId(Long orderId);
}
