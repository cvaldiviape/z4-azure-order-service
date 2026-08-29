package com.z4greed.order.service.saga.purchase;

public interface PurchaseSagaService {
  void handleInventoryEvent(String rawEvent);
  void handlePaymentEvent(String rawEvent);
}
