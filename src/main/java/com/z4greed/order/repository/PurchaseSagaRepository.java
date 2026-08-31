package com.z4greed.order.repository;

import com.z4greed.order.entity.PurchaseSagaEntity;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PurchaseSagaRepository extends JpaRepository<PurchaseSagaEntity, Long> {
  Optional<PurchaseSagaEntity> findByOrderId(Long orderId);
}
