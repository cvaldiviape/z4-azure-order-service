package com.z4greed.order.service;

import com.z4greed.order.entity.OutboxEventEntity;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import java.util.List;

public interface OutboxEventService {
  void enqueue(String topic, EventEnvelopeDto eventEnvelopeDto);
  List<OutboxEventEntity> findPending(int batchSize);
  void markAsPublished(String eventId);
  void registerFailedAttempt(String eventId, String errorMessage);
}