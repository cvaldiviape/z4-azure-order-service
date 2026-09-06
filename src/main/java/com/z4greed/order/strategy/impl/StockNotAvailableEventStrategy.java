package com.z4greed.order.strategy.impl;

import com.z4greed.order.dto.PurchaseSagaContextDto;
import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.enums.OrderStatusEnum;
import com.z4greed.order.enums.SagaStatusEnum;
import com.z4greed.order.kafka.factory.OrderEventFactory;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import com.z4greed.order.service.OutboxEventService;
import com.z4greed.order.strategy.EventStrategy;
import com.z4greed.order.saga.state.PurchaseSagaStateManager;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class StockNotAvailableEventStrategy implements EventStrategy<EventTypeEnum, PurchaseSagaContextDto> {
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
    String CANCELLATION_REASON = "Stock not available";

    this.purchaseSagaStateManager.update(purchaseSagaContextDto, OrderStatusEnum.CANCELLED, SagaStatusEnum.COMPLETED, EventTypeEnum.ORDER_CANCELLED, CANCELLATION_REASON);

    OrderEntity orderEntity = purchaseSagaContextDto.orderEntity();
    String causationId = purchaseSagaContextDto.sourceEvent().eventId();
    Map<String, Object> mapPayload = Map.of(
            "reason", CANCELLATION_REASON
    );

    EventEnvelopeDto eventEnvelopeDto = this.orderEventFactory.build(EventTypeEnum.ORDER_CANCELLED, orderEntity, causationId, mapPayload);

    this.outboxEventService.enqueue("orders-events-topic", eventEnvelopeDto);
  }

}