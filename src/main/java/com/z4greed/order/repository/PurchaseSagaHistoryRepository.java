package com.z4greed.order.repository;

import com.z4greed.order.entity.PurchaseSagaHistoryEntity;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PurchaseSagaHistoryRepository extends JpaRepository<PurchaseSagaHistoryEntity, Long> {
    List<PurchaseSagaHistoryEntity> findByOrderIdOrderByCreatedAtAscIdAsc(Long orderId);
}
