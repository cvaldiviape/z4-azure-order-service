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
public class StockReservedEventStrategy implements PurchaseSagaEventStrategy {
  private static final String PAYMENTS_TOPIC = "payments-events-topic";

  private final PurchaseSagaStateManager purchaseSagaStateManager;
  private final OrderEventFactory orderEventFactory;
  private final OrderEventProducer orderEventProducer;

  public StockReservedEventStrategy(
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
    return EventTypeEnum.STOCK_RESERVED;
  }

  @Override
  public void execute(PurchaseSagaContextDto purchaseSagaContextDto) {
    this.purchaseSagaStateManager.update(purchaseSagaContextDto, OrderStatusEnum.PAYMENT_PENDING, SagaStatusEnum.IN_PROGRESS, EventTypeEnum.STOCK_RESERVED, null);

    OrderEntity orderEntity = purchaseSagaContextDto.orderEntity();
    String causationId = purchaseSagaContextDto.sourceEvent().eventId();
    Map<String, Object> mapPayload = this.buildMapPayload(orderEntity);

    EventEnvelopeDto eventEnvelopeDto = this.orderEventFactory.build(EventTypeEnum.PAYMENT_REQUESTED, orderEntity, causationId, mapPayload);

    this.orderEventProducer.publish(PAYMENTS_TOPIC, eventEnvelopeDto);
  }

  private Map<String, Object> buildMapPayload(OrderEntity orderEntity) {
    return Map.of(
        "customerId", orderEntity.getCustomerId(),
        "amount", orderEntity.getTotalAmount(),
        "currency", orderEntity.getCurrency(),
        "paymentToken", orderEntity.getPaymentToken()
    );
  }

}