package com.z4greed.order.service.impl;

import com.z4greed.order.entity.OutboxEventEntity;
import com.z4greed.order.enums.*;
import com.z4greed.order.exception.CustomNonRetryableKafkaException;
import com.z4greed.order.kafka.event.IntegrationEventDto;
import com.z4greed.order.repository.OutboxEventRepository;
import com.z4greed.order.service.OutboxEventService;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

@Service
public class OutboxEventServiceImpl implements OutboxEventService {
  private final OutboxEventRepository outboxEventRepository;
  private final ObjectMapper mapper;

  public OutboxEventServiceImpl(OutboxEventRepository outboxEventRepository, ObjectMapper mapper) {
    this.outboxEventRepository = outboxEventRepository;
    this.mapper = mapper;
  }

  @Override @Transactional
  public void enqueue(String topic, IntegrationEventDto event) {
    String payload = this.serialize(event);

    OutboxEventEntity outboxEventEntity = OutboxEventEntity.builder()
            .eventId(event.eventId())
            .aggregateId(event.aggregateId())
            .correlationId(event.correlationId())
            .eventType(event.eventType())
            .topic(topic)
            .eventKey(event.aggregateId())
            .payload(payload)
            .status(OutboxStatusEnum.PENDING)
            .attempts(0).createdAt(LocalDateTime.now())
            .build();

    this.outboxEventRepository.save(outboxEventEntity);
  }

  private String serialize(IntegrationEventDto event) {
    try { return this.mapper.writeValueAsString(event); }
    catch (Exception exception) {
      throw new CustomNonRetryableKafkaException(ErrorCodeEnum.OUTBOX_SERIALIZATION_FAILED, exception);
    }
  }

  @Override @Transactional(readOnly = true)
  public List<OutboxEventEntity> findPending(int batchSize) {
    PageRequest pageable = PageRequest.of(0, batchSize);
    return this.outboxEventRepository.findByStatusOrderByCreatedAtAsc(OutboxStatusEnum.PENDING, pageable);
  }

  @Override @Transactional
  public void markAsPublished(String eventId) {
    Optional<OutboxEventEntity> outboxEventEntity = this.outboxEventRepository.findById(eventId);

    outboxEventEntity.ifPresent(event -> {
      event.setStatus(OutboxStatusEnum.PUBLISHED);
      event.setPublishedAt(LocalDateTime.now());
      event.setLastError(null);
    });
  }

  @Override @Transactional
  public void registerFailedAttempt(String eventId, String errorMessage) {
    Optional<OutboxEventEntity> outboxEventEntity = this.outboxEventRepository.findById(eventId);

    outboxEventEntity.ifPresent(event -> {
      event.setAttempts(event.getAttempts() + 1);
      event.setLastError(errorMessage);
    });
  }

}
