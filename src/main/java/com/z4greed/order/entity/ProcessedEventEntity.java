package com.z4greed.order.entity;

import com.z4greed.order.kafka.event.EventEnvelopeDto;
import jakarta.persistence.*;
import java.time.Instant;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "processed_events")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ProcessedEventEntity {
  @Id private String eventId;
  private String eventType;
  private Instant processedAt;

  public ProcessedEventEntity(EventEnvelopeDto eventEnvelopeDto) {
    this.eventId = eventEnvelopeDto.eventId();
    this.eventType = eventEnvelopeDto.eventType();
    this.processedAt = Instant.now();
  }
}
