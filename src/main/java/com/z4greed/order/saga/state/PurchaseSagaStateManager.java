package com.z4greed.order.saga.state;

import com.z4greed.order.dto.PurchaseSagaContextDto;
import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.entity.PurchaseSagaEntity;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.enums.OrderStatusEnum;
import com.z4greed.order.enums.SagaStatusEnum;
import java.time.LocalDateTime;
import java.util.Set;
import org.springframework.stereotype.Component;

@Component
public class PurchaseSagaStateManager {
    // Estados en los que el pago ya no se volverá a intentar, por lo que el token deja de ser necesario.
    private static final Set<SagaStatusEnum> STATUSES_WITHOUT_PENDING_PAYMENT = Set.of(
            SagaStatusEnum.COMPLETED,
            SagaStatusEnum.COMPENSATING,
            SagaStatusEnum.FAILED);

    private final PurchaseSagaHistoryRecorder purchaseSagaHistoryRecorder;

    public PurchaseSagaStateManager(PurchaseSagaHistoryRecorder purchaseSagaHistoryRecorder) {
        this.purchaseSagaHistoryRecorder = purchaseSagaHistoryRecorder;
    }

    // orderStatusEnum puede ser null cuando el paso de la Saga no cambia el estado del pedido.
    public void update(PurchaseSagaContextDto purchaseSagaContextDto, OrderStatusEnum orderStatusEnum, SagaStatusEnum sagaStatusEnum, EventTypeEnum eventTypeEnum, String errorMessage) {
        LocalDateTime updatedAt = LocalDateTime.now();
        OrderEntity orderEntity = purchaseSagaContextDto.orderEntity();
        PurchaseSagaEntity purchaseSagaEntity = purchaseSagaContextDto.purchaseSagaEntity();
        String eventId = purchaseSagaContextDto.event().eventId();

        this.updateOrder(orderEntity, orderStatusEnum, updatedAt);
        this.updateSaga(purchaseSagaEntity, sagaStatusEnum, eventTypeEnum, eventId, errorMessage, updatedAt);
        this.clearPaymentTokenWhenNoLongerNeeded(orderEntity, sagaStatusEnum);
        this.purchaseSagaHistoryRecorder.record(purchaseSagaEntity, eventTypeEnum, eventId, updatedAt);
    }

    private void updateOrder(OrderEntity orderEntity, OrderStatusEnum orderStatusEnum, LocalDateTime updatedAt) {
        if (orderStatusEnum == null) {
            return;
        }
        orderEntity.setStatus(orderStatusEnum);
        orderEntity.setUpdatedAt(updatedAt);
    }

    private void updateSaga(PurchaseSagaEntity purchaseSagaEntity, SagaStatusEnum sagaStatusEnum, EventTypeEnum eventTypeEnum, String eventId, String errorMessage, LocalDateTime updatedAt) {
        purchaseSagaEntity.setStatus(sagaStatusEnum);
        purchaseSagaEntity.setCurrentStep(eventTypeEnum.getValue());
        purchaseSagaEntity.setLastEventId(eventId);
        purchaseSagaEntity.setErrorMessage(errorMessage);
        purchaseSagaEntity.setUpdatedAt(updatedAt);
        purchaseSagaEntity.setTimedOutAt(null);
    }

    private void clearPaymentTokenWhenNoLongerNeeded(OrderEntity orderEntity, SagaStatusEnum sagaStatusEnum) {
        if (STATUSES_WITHOUT_PENDING_PAYMENT.contains(sagaStatusEnum)) {
            orderEntity.setPaymentToken(null);
        }
    }

}
