package com.z4greed.order.entity;

import jakarta.persistence.*;
import java.math.BigDecimal;
import lombok.*;

@Entity
@Table(name = "order_items")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OrderItemEntity {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @ManyToOne
  @JoinColumn(name = "order_id")
  private OrderEntity order;

  private Long productId;
  private String productName;
  private BigDecimal unitPrice;
  private int quantity;
  private BigDecimal subtotal;

  @Builder
  public OrderItemEntity(
      OrderEntity orderEntity,
      Long productId,
      String productName,
      BigDecimal unitPrice,
      int quantity) {
    this.order = orderEntity;
    this.productId = productId;
    this.productName = productName;
    this.unitPrice = unitPrice;
    this.quantity = quantity;
    this.subtotal = unitPrice.multiply(BigDecimal.valueOf(quantity));
  }
}
