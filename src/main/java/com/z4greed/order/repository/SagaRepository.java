package com.z4greed.order.repository;

import com.z4greed.order.entity.OrderSagaEntity;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SagaRepository extends JpaRepository<OrderSagaEntity, Long> {
  Optional<OrderSagaEntity> findByOrderId(Long orderId);
}
