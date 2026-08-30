package com.z4greed.order.service.saga.purchase.strategy.impl;

import com.z4greed.order.dto.PurchaseSagaContextDto;
import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.enums.SagaStatusEnum;
import com.z4greed.order.kafka.factory.OrderEventFactory;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import com.z4greed.order.kafka.producer.OrderEventProducer;
import com.z4greed.order.service.saga.purchase.strategy.PurchaseSagaEventStrategy;
import com.z4greed.order.service.saga.purchase.state.PurchaseSagaStateManager;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class PaymentFailedEventStrategy implements PurchaseSagaEventStrategy {
  private final PurchaseSagaStateManager purchaseSagaStateManager;
  private final OrderEventFactory orderEventFactory;
  private final OrderEventProducer orderEventProducer;

  public PaymentFailedEventStrategy(
      PurchaseSagaStateManager purchaseSagaStateManager,
      OrderEventFactory orderEventFactory,
      OrderEventProducer orderEventProducer
  ) {
    this.purchaseSagaStateManager = purchaseSagaStateManager;
    this.orderEventFactory = orderEventFactory;
    this.orderEventProducer = orderEventProducer;
  }

  @Override
  public EventTypeEnum getEventType() {
    return EventTypeEnum.PAYMENT_FAILED;
  }

  @Override
  public void execute(PurchaseSagaContextDto purchaseSagaContextDto) {
    this.purchaseSagaStateManager.update(purchaseSagaContextDto, null, SagaStatusEnum.COMPENSATING, EventTypeEnum.PAYMENT_FAILED, null);

    OrderEntity orderEntity = purchaseSagaContextDto.orderEntity();
    String causationId = purchaseSagaContextDto.sourceEvent().eventId();
    Map<String, Object> mapPayload = Map.of();

    EventEnvelopeDto eventEnvelopeDto = this.orderEventFactory.build(EventTypeEnum.RELEASE_STOCK, orderEntity, causationId, mapPayload);

    this.orderEventProducer.publish("inventory-events-topic", eventEnvelopeDto);
  }

}
