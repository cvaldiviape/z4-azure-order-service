package com.z4greed.order.entity;

import com.z4greed.order.enums.OrderStatusEnum;
import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "orders")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
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
  private Instant createdAt;
  private Instant updatedAt;

  @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, fetch = FetchType.EAGER)
  private List<OrderItemEntity> listItems = new ArrayList<>();

  public static OrderEntity create(Long customerId, String paymentToken) {
    OrderEntity orderEntity = new OrderEntity();
    orderEntity.customerId = customerId;
    orderEntity.paymentToken = paymentToken;
    orderEntity.status = OrderStatusEnum.CREATED;
    orderEntity.currency = "PEN";
    orderEntity.correlationId = "purchase-" + UUID.randomUUID();
    orderEntity.createdAt = Instant.now();
    return orderEntity;
  }

  public void addItem(OrderItemEntity orderItemEntity) {
    this.listItems.add(orderItemEntity);
  }

  public void calculateTotal() {
    this.totalAmount =
        this.listItems.stream()
            .map(OrderItemEntity::getSubtotal)
            .reduce(BigDecimal.ZERO, BigDecimal::add);
  }

  public void changeStatus(OrderStatusEnum orderStatus) {
    this.status = orderStatus;
    this.updatedAt = Instant.now();
  }

  public List<OrderItemEntity> getListItems() {
    return Collections.unmodifiableList(this.listItems);
  }
}
