package com.z4greed.order.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import lombok.*;

@Entity
@Table(name = "inbox_events")
@Getter
@Setter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class InboxEventEntity {
  @Id private String eventId;
  private String eventType;
  private String aggregateId;
  private String correlationId;
  private String previousEventId;
  private String producer;
  private String sourceTopic;
  private LocalDateTime processedAt;
}
