package com.z4greed.order.entity;

import com.z4greed.order.enums.OutboxStatusEnum;
import jakarta.persistence.*;
import java.time.LocalDateTime;
import lombok.*;

@Entity
@Table(name = "outbox_events")
@Getter @Setter @Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class OutboxEventEntity {
  @Id private String eventId;
  private String aggregateId;
  private String correlationId;
  private String eventType;
  private String topic;
  private String eventKey;
  @Column(columnDefinition = "TEXT") private String payload;
  @Enumerated(EnumType.STRING) private OutboxStatusEnum status;
  private Integer attempts;
  private LocalDateTime createdAt;
  private LocalDateTime publishedAt;
  @Column(columnDefinition = "TEXT") private String lastError;
}
