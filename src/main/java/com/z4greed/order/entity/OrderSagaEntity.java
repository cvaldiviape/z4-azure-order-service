package com.z4greed.order.entity;

import com.z4greed.order.enums.SagaStatusEnum;
import jakarta.persistence.*;
import java.time.LocalDateTime;
import lombok.*;

@Entity
@Table(name = "order_sagas")
@Getter
@Setter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
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
  private LocalDateTime createdAt;
  private LocalDateTime updatedAt;
}
