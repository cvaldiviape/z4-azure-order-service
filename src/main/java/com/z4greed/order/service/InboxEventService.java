package com.z4greed.order.service;

import com.z4greed.order.kafka.event.EventEnvelopeDto;

public interface InboxEventService {
  boolean wasAlreadyProcessed(String eventId);
  void register(EventEnvelopeDto event, String sourceTopic);
}
