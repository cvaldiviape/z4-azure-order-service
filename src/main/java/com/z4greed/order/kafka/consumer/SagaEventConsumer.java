package com.z4greed.order.kafka.consumer;

import com.z4greed.order.service.saga.purchase.PurchaseSagaService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SagaEventConsumer {
  private final PurchaseSagaService purchaseSagaService;

  public SagaEventConsumer(
      PurchaseSagaService purchaseSagaService
  ) {
    this.purchaseSagaService = purchaseSagaService;
  }

  @KafkaListener(topics = "inventory.events")
  public void inventory(String rawEvent) {
    this.purchaseSagaService.handleInventoryEvent(rawEvent);
  }

  @KafkaListener(topics = "payments.events")
  public void payment(String rawEvent) {
    this.purchaseSagaService.handlePaymentEvent(rawEvent);
  }

}
