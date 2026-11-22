package com.z4greed.order.saga.impl;

import com.z4greed.shared.core.dto.PageResponseDto;
import com.z4greed.order.dto.PurchaseSagaDetailDto;
import com.z4greed.order.dto.PurchaseSagaHistoryDto;
import com.z4greed.order.dto.PurchaseSagaSummaryDto;
import com.z4greed.order.entity.PurchaseSagaEntity;
import com.z4greed.order.enums.ErrorCodeEnum;
import com.z4greed.order.enums.SagaStatusEnum;
import com.z4greed.shared.core.exception.BusinessException;
import com.z4greed.order.repository.PurchaseSagaHistoryRepository;
import com.z4greed.order.repository.PurchaseSagaRepository;
import com.z4greed.order.saga.PurchaseSagaQueryService;
import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional(readOnly = true)
public class PurchaseSagaQueryServiceImpl implements PurchaseSagaQueryService {
    private final PurchaseSagaRepository purchaseSagaRepository;
    private final PurchaseSagaHistoryRepository purchaseSagaHistoryRepository;

    public PurchaseSagaQueryServiceImpl(
            PurchaseSagaRepository purchaseSagaRepository,
            PurchaseSagaHistoryRepository purchaseSagaHistoryRepository
    ) {
        this.purchaseSagaRepository = purchaseSagaRepository;
        this.purchaseSagaHistoryRepository = purchaseSagaHistoryRepository;
    }

    // Sin status devuelve todas las Sagas; con status filtra (ej. TIMED_OUT para ver las detenidas).
    @Override
    public PageResponseDto<PurchaseSagaSummaryDto> search(SagaStatusEnum status, Pageable pageable) {
        Page<PurchaseSagaEntity> sagas = status == null
                ? this.purchaseSagaRepository.findAll(pageable)
                : this.purchaseSagaRepository.findByStatus(status, pageable);
        return PageResponseDto.from(sagas.map(PurchaseSagaSummaryDto::from));
    }

    @Override
    public PurchaseSagaDetailDto getByOrderId(Long orderId) {
        PurchaseSagaEntity purchaseSagaEntity = this.purchaseSagaRepository.findByOrderId(orderId)
                .orElseThrow(() -> new BusinessException(ErrorCodeEnum.PURCHASE_SAGA_NOT_FOUND));

        List<PurchaseSagaHistoryDto> timeline = this.purchaseSagaHistoryRepository.findByOrderIdOrderByCreatedAtAscIdAsc(orderId)
                .stream()
                .map(PurchaseSagaHistoryDto::from)
                .toList();

        return new PurchaseSagaDetailDto(PurchaseSagaSummaryDto.from(purchaseSagaEntity), timeline);
    }
}
