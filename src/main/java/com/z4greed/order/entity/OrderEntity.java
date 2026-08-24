package com.z4greed.order.entity;

import com.z4greed.order.enums.OrderStatusEnum;
import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import lombok.*;

@Entity
@Table(name = "orders")
@Getter
@Setter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class OrderEntity {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  private Long customerId;

  @Enumerated(EnumType.STRING)
  private OrderStatusEnum status;

  private BigDecimal totalAmount;
  private String currency;
  private String correlationId;
  private String paymentToken;
  private LocalDateTime createdAt;
  private LocalDateTime updatedAt;

  @Builder.Default
  @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, fetch = FetchType.EAGER)
  private List<OrderItemEntity> listItems = new ArrayList<>();
}
