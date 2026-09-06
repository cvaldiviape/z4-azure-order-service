package com.z4greed.order.repository;

import com.z4greed.order.entity.PurchaseSagaEntity;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface PurchaseSagaRepository extends JpaRepository<PurchaseSagaEntity, Long> {
  Optional<PurchaseSagaEntity> findByOrderId(Long orderId);

  @Query(value = """
      SELECT *
      FROM purchase_sagas
      WHERE status IN ('STARTED', 'IN_PROGRESS', 'COMPENSATING')
        AND COALESCE(updated_at, created_at) < :cutoff
      ORDER BY COALESCE(updated_at, created_at), id
      LIMIT :batchSize
      FOR UPDATE SKIP LOCKED
      """, nativeQuery = true)
  List<PurchaseSagaEntity> findStaleForUpdate(
      @Param("cutoff") LocalDateTime cutoff,
      @Param("batchSize") int batchSize);
}
