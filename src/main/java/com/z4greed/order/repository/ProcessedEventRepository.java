package com.z4greed.order.repository;

import com.z4greed.order.entity.ProcessedEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEventEntity, String> {}
