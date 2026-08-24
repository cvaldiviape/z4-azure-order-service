package com.z4greed.order.entity;

import com.z4greed.order.enums.SagaStatusEnum;
import jakarta.persistence.*;
import java.time.Instant;
import lombok.*;

@Entity
@Table(name = "order_saga")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OrderSagaEntity {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @OneToOne
  @JoinColumn(name = "order_id")
  private OrderEntity order;

  @Enumerated(EnumType.STRING)
  private SagaStatusEnum status;

  private String currentStep;
  private String lastEventId;
  private String errorMessage;
  private Instant createdAt;
  private Instant updatedAt;

  @Builder
  public OrderSagaEntity(OrderEntity orderEntity) {
    this.order = orderEntity;
    this.status = SagaStatusEnum.STARTED;
    this.currentStep = "ORDER_CREATED";
    this.createdAt = Instant.now();
  }

  public void transition(SagaStatusEnum status, String step, String eventId) {
    this.status = status;
    this.currentStep = step;
    this.lastEventId = eventId;
    this.updatedAt = Instant.now();
  }

  public void completeCancellation(String eventId, String error) {
    this.transition(SagaStatusEnum.COMPLETED, "ORDER_CANCELLED", eventId);
    this.errorMessage = error;
  }
}
