package com.z4greed.order.strategy.impl;

import com.z4greed.order.dto.PurchaseSagaContextDto;
import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.enums.OrderStatusEnum;
import com.z4greed.order.enums.SagaStatusEnum;
import com.z4greed.order.kafka.factory.OrderEventFactory;
import com.z4greed.order.kafka.event.IntegrationEventDto;
import com.z4greed.order.service.OutboxEventService;
import com.z4greed.order.strategy.EventStrategy;
import com.z4greed.order.saga.state.PurchaseSagaStateManager;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class StockReservedEventStrategy implements EventStrategy<EventTypeEnum, PurchaseSagaContextDto> {
  private final PurchaseSagaStateManager purchaseSagaStateManager;
  private final OrderEventFactory orderEventFactory;
  private final OutboxEventService outboxEventService;

  public StockReservedEventStrategy(
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
    return EventTypeEnum.STOCK_RESERVED;
  }

  @Override
  public void execute(PurchaseSagaContextDto purchaseSagaContextDto) {
    this.purchaseSagaStateManager.update(purchaseSagaContextDto, OrderStatusEnum.PAYMENT_PENDING, SagaStatusEnum.IN_PROGRESS, EventTypeEnum.STOCK_RESERVED, null);

    OrderEntity orderEntity = purchaseSagaContextDto.orderEntity();
    String previousEventId = purchaseSagaContextDto.event().eventId();
    Map<String, Object> mapPayload =  Map.of(
            "customerId", orderEntity.getCustomerId(),
            "amount", orderEntity.getTotalAmount(),
            "currency", orderEntity.getCurrency(),
            "paymentToken", orderEntity.getPaymentToken()
    );

    IntegrationEventDto paymentRequestedEvent = this.orderEventFactory.build(EventTypeEnum.PAYMENT_REQUESTED, orderEntity, previousEventId, mapPayload);

    this.outboxEventService.enqueue("payment-processing-requests-topic", paymentRequestedEvent);
  }

}
