package com.z4greed.order.saga.state;

import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.entity.PurchaseSagaEntity;
import com.z4greed.order.entity.PurchaseSagaHistoryEntity;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.repository.PurchaseSagaHistoryRepository;
import java.time.LocalDateTime;
import org.springframework.stereotype.Component;

// Guarda una fotografía del estado del pedido y de la Saga cada vez que avanza.
// Este historial es la fuente que usa la Ops Console para mostrar la línea de tiempo de cada pedido.
@Component
public class PurchaseSagaHistoryRecorder {
    private final PurchaseSagaHistoryRepository purchaseSagaHistoryRepository;

    public PurchaseSagaHistoryRecorder(PurchaseSagaHistoryRepository purchaseSagaHistoryRepository) {
        this.purchaseSagaHistoryRepository = purchaseSagaHistoryRepository;
    }

    public void record(PurchaseSagaEntity purchaseSagaEntity, EventTypeEnum eventTypeEnum, String eventId, LocalDateTime createdAt) {
        OrderEntity orderEntity = purchaseSagaEntity.getOrder();

        PurchaseSagaHistoryEntity purchaseSagaHistoryEntity = PurchaseSagaHistoryEntity.builder()
                .purchaseSaga(purchaseSagaEntity)
                .orderId(orderEntity.getId())
                .orderStatus(orderEntity.getStatus())
                .sagaStatus(purchaseSagaEntity.getStatus())
                .eventType(eventTypeEnum)
                .eventId(eventId)
                .errorMessage(purchaseSagaEntity.getErrorMessage())
                .createdAt(createdAt)
                .build();

        this.purchaseSagaHistoryRepository.save(purchaseSagaHistoryEntity);
    }
}
