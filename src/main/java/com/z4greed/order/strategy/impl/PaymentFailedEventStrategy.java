package com.z4greed.order.strategy.impl;

import com.z4greed.order.dto.PurchaseSagaContextDto;
import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.enums.SagaStatusEnum;
import com.z4greed.order.kafka.KafkaTopics;
import com.z4greed.order.kafka.factory.OrderEventFactory;
import com.z4greed.shared.kafka.event.IntegrationEventDto;
import com.z4greed.shared.kafka.outbox.OutboxEventService;
import com.z4greed.order.strategy.EventStrategy;
import com.z4greed.order.saga.state.PurchaseSagaStateManager;
import org.springframework.stereotype.Component;

@Component
public class PaymentFailedEventStrategy implements EventStrategy<EventTypeEnum, PurchaseSagaContextDto> {
    private final PurchaseSagaStateManager purchaseSagaStateManager;
    private final OrderEventFactory orderEventFactory;
    private final OutboxEventService outboxEventService;

    public PaymentFailedEventStrategy(
            PurchaseSagaStateManager purchaseSagaStateManager,
            OrderEventFactory orderEventFactory,
            OutboxEventService outboxEventService
    ) {
        this.purchaseSagaStateManager = purchaseSagaStateManager;
        this.orderEventFactory = orderEventFactory;
        this.outboxEventService = outboxEventService;
    }

    @Override
    public EventTypeEnum getEventType() {
        return EventTypeEnum.PAYMENT_FAILED;
    }

    @Override
    public void execute(PurchaseSagaContextDto purchaseSagaContextDto) {
        this.purchaseSagaStateManager.update(purchaseSagaContextDto, null, SagaStatusEnum.COMPENSATING, EventTypeEnum.PAYMENT_FAILED, null);
        this.publishReleaseStock(purchaseSagaContextDto);
    }

    private void publishReleaseStock(PurchaseSagaContextDto purchaseSagaContextDto) {
        OrderEntity orderEntity = purchaseSagaContextDto.orderEntity();
        String previousEventId = purchaseSagaContextDto.event().eventId();

        IntegrationEventDto releaseStockEvent = this.orderEventFactory.build(EventTypeEnum.RELEASE_STOCK, orderEntity, previousEventId);

        this.outboxEventService.enqueue(KafkaTopics.INVENTORY_RESERVATION_REQUESTS, releaseStockEvent);
    }

}
