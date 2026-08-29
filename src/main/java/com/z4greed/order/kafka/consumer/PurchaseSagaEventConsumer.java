package com.z4greed.order.kafka.consumer;

import com.z4greed.order.service.saga.purchase.PurchaseSagaService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PurchaseSagaEventConsumer {
  private final PurchaseSagaService purchaseSagaService;

  public PurchaseSagaEventConsumer(PurchaseSagaService purchaseSagaService) {
    this.purchaseSagaService = purchaseSagaService;
  }

  // Permanece a la escucha de los eventos de inventario publicados en "inventory-events-topic".
  @KafkaListener(topics = "inventory-events-topic")
  public void consumeInventoryEvents(String rawEvent) {
    this.purchaseSagaService.handleInventoryEvent(rawEvent);
  }

  // Permanece a la escucha de los eventos de pago publicados en "payments-events-topic".
  @KafkaListener(topics = "payments-events-topic")
  public void consumePaymentEvents(String rawEvent) {
    this.purchaseSagaService.handlePaymentEvent(rawEvent);
  }

}