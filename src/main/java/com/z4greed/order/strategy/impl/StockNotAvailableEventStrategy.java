package com.z4greed.order.strategy.impl;

import com.z4greed.order.dto.PurchaseSagaContextDto;
import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.enums.OrderStatusEnum;
import com.z4greed.order.enums.SagaStatusEnum;
import com.z4greed.order.kafka.KafkaTopics;
import com.z4greed.order.kafka.factory.OrderEventFactory;
import com.z4greed.shared.kafka.event.IntegrationEventDto;
import com.z4greed.shared.kafka.outbox.OutboxEventService;
import com.z4greed.order.strategy.EventStrategy;
import com.z4greed.order.saga.state.PurchaseSagaStateManager;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class StockNotAvailableEventStrategy implements EventStrategy<EventTypeEnum, PurchaseSagaContextDto> {
    private static final String CANCELLATION_REASON = "Stock not available";

    private final PurchaseSagaStateManager purchaseSagaStateManager;
    private final OrderEventFactory orderEventFactory;
    private final OutboxEventService outboxEventService;

    public StockNotAvailableEventStrategy(
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
        return EventTypeEnum.STOCK_NOT_AVAILABLE;
    }

    @Override
    public void execute(PurchaseSagaContextDto purchaseSagaContextDto) {
        this.purchaseSagaStateManager.update(purchaseSagaContextDto, OrderStatusEnum.CANCELLED, SagaStatusEnum.COMPLETED, EventTypeEnum.ORDER_CANCELLED, CANCELLATION_REASON);
        this.publishOrderCancelled(purchaseSagaContextDto);
    }

    private void publishOrderCancelled(PurchaseSagaContextDto purchaseSagaContextDto) {
        OrderEntity orderEntity = purchaseSagaContextDto.orderEntity();
        String previousEventId = purchaseSagaContextDto.event().eventId();
        Map<String, Object> mapPayload = Map.of(
                "reason", CANCELLATION_REASON
        );

        IntegrationEventDto orderCancelledEvent = this.orderEventFactory.build(EventTypeEnum.ORDER_CANCELLED, orderEntity, previousEventId, mapPayload);

        this.outboxEventService.enqueue(KafkaTopics.ORDER_STATUS_EVENTS, orderCancelledEvent);
    }

}
