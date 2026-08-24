package com.z4greed.order.service;

public interface OrderSagaService {
  void handleInventoryEvent(String rawEvent);
  void handlePaymentEvent(String rawEvent);
}