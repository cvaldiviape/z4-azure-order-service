package com.z4greed.order.entity;

import com.z4greed.order.enums.OrderStatusEnum;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.order.enums.SagaStatusEnum;
import jakarta.persistence.*;
import java.time.LocalDateTime;
import lombok.*;

@Entity
@Table(name = "purchase_saga_histories")
@Getter
@Setter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class PurchaseSagaHistoryEntity {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "purchase_saga_id", nullable = false)
  private PurchaseSagaEntity purchaseSaga;

  private Long orderId;

  @Enumerated(EnumType.STRING)
  private OrderStatusEnum orderStatus;

  @Enumerated(EnumType.STRING)
  private SagaStatusEnum sagaStatus;

  @Enumerated(EnumType.STRING)
  private EventTypeEnum eventType;

  private String eventId;
  private String errorMessage;
  private LocalDateTime createdAt;
}
