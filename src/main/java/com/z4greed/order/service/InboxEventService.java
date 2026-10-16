package com.z4greed.order.service;

import com.z4greed.order.kafka.event.IntegrationEventDto;

public interface InboxEventService {
  boolean wasAlreadyProcessed(String eventId);
  void register(IntegrationEventDto event, String sourceTopic);
}
