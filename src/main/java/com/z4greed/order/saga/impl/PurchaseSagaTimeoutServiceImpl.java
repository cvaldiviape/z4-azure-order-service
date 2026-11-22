package com.z4greed.order.saga.impl;

import com.z4greed.order.entity.PurchaseSagaEntity;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.enums.SagaStatusEnum;
import com.z4greed.order.repository.PurchaseSagaRepository;
import com.z4greed.order.saga.PurchaseSagaTimeoutService;
import com.z4greed.order.saga.state.PurchaseSagaHistoryRecorder;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PurchaseSagaTimeoutServiceImpl implements PurchaseSagaTimeoutService {
    private static final String TIMEOUT_ERROR_PREFIX = "Saga timed out while waiting after step ";
    private static final String TIMEOUT_EVENT_ID_PREFIX = "saga-timeout-";

    private final PurchaseSagaRepository purchaseSagaRepository;
    private final PurchaseSagaHistoryRecorder purchaseSagaHistoryRecorder;

    public PurchaseSagaTimeoutServiceImpl(
            PurchaseSagaRepository purchaseSagaRepository,
            PurchaseSagaHistoryRecorder purchaseSagaHistoryRecorder
    ) {
        this.purchaseSagaRepository = purchaseSagaRepository;
        this.purchaseSagaHistoryRecorder = purchaseSagaHistoryRecorder;
    }

    @Override
    @Transactional
    public int timeoutStaleSagas(LocalDateTime cutoff, int batchSize) {
        List<PurchaseSagaEntity> staleSagas = this.purchaseSagaRepository.findStaleForUpdate(cutoff, batchSize);
        staleSagas.forEach(this::markAsTimedOut);
        return staleSagas.size();
    }

    // Solo cambia la Saga: el pedido y su currentStep se conservan para que la reconciliación sepa dónde se detuvo.
    private void markAsTimedOut(PurchaseSagaEntity purchaseSagaEntity) {
        LocalDateTime timedOutAt = LocalDateTime.now();
        String timeoutEventId = TIMEOUT_EVENT_ID_PREFIX + UUID.randomUUID();

        purchaseSagaEntity.setStatus(SagaStatusEnum.TIMED_OUT);
        purchaseSagaEntity.setErrorMessage(TIMEOUT_ERROR_PREFIX + purchaseSagaEntity.getCurrentStep());
        purchaseSagaEntity.setUpdatedAt(timedOutAt);
        purchaseSagaEntity.setTimedOutAt(timedOutAt);

        this.purchaseSagaHistoryRecorder.record(purchaseSagaEntity, EventTypeEnum.SAGA_TIMED_OUT, timeoutEventId, timedOutAt);
    }
}
