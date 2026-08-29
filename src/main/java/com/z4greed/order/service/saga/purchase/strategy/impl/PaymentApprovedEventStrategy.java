package com.z4greed.order.service.saga.purchase.strategy.impl;

import com.z4greed.order.dto.PurchaseSagaContextDto;
import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.enums.OrderStatusEnum;
import com.z4greed.order.enums.SagaStatusEnum;
import com.z4greed.order.kafka.factory.OrderEventFactory;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import com.z4greed.order.kafka.producer.OrderEventProducer;
import com.z4greed.order.service.saga.purchase.strategy.PurchaseSagaEventStrategy;
import com.z4greed.order.service.saga.purchase.state.PurchaseSagaStateManager;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class PaymentApprovedEventStrategy implements PurchaseSagaEventStrategy {
  private static final String ORDERS_TOPIC = "orders-events-topic";

  private final PurchaseSagaStateManager purchaseSagaStateManager;
  private final OrderEventFactory orderEventFactory;
  private final OrderEventProducer orderEventProducer;

  public PaymentApprovedEventStrategy(
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
    return EventTypeEnum.PAYMENT_APPROVED;
  }

  @Override
  public void execute(PurchaseSagaContextDto purchaseSagaContextDto) {
    this.purchaseSagaStateManager.update(purchaseSagaContextDto, OrderStatusEnum.CONFIRMED, SagaStatusEnum.COMPLETED, EventTypeEnum.PAYMENT_APPROVED, null);

    OrderEntity orderEntity = purchaseSagaContextDto.orderEntity();
    String causationId = purchaseSagaContextDto.sourceEvent().eventId();
    Map<String, Object> mapPayload = Map.of();

    EventEnvelopeDto eventEnvelopeDto = this.orderEventFactory.build(EventTypeEnum.ORDER_CONFIRMED, orderEntity, causationId, mapPayload);

    this.orderEventProducer.publish(ORDERS_TOPIC, eventEnvelopeDto);
  }

}