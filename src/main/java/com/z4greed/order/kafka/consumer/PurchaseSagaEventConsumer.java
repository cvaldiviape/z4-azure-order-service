package com.z4greed.order.kafka.consumer;

import com.z4greed.order.saga.PurchaseSagaService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PurchaseSagaEventConsumer {
  private final PurchaseSagaService purchaseSagaService;

  public PurchaseSagaEventConsumer(PurchaseSagaService purchaseSagaService) {
    this.purchaseSagaService = purchaseSagaService;
  }

  // Permanece a la escucha de los resultados de reserva publicados por inventory-service.
  @KafkaListener(topics = "inventory-reservation-results-topic")
  public void consumeInventoryEvents(String rawEvent) {
    this.purchaseSagaService.handleInventoryEvent(rawEvent);
  }

  // Permanece a la escucha de los resultados de pago publicados por payment-service.
  @KafkaListener(topics = "payment-processing-results-topic")
  public void consumePaymentEvents(String rawEvent) {
    this.purchaseSagaService.handlePaymentEvent(rawEvent);
  }

}
