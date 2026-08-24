package com.z4greed.order.kafka.consumer;

import com.z4greed.order.service.OrderSagaService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SagaEventConsumer {
  private final OrderSagaService orderSagaService;

  public SagaEventConsumer(OrderSagaService orderSagaService) {
    this.orderSagaService = orderSagaService;
  }

  @KafkaListener(topics = "inventory.events")
  public void inventory(String rawEvent) {
    this.orderSagaService.handleInventoryEvent(rawEvent);
  }

  @KafkaListener(topics = "payments.events")
  public void payment(String rawEvent) {
    this.orderSagaService.handlePaymentEvent(rawEvent);
  }

}