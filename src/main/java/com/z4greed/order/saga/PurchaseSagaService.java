package com.z4greed.order.saga;

public interface PurchaseSagaService {
  void handleInventoryEvent(String rawEvent);
  void handlePaymentEvent(String rawEvent);
}