package com.z4greed.order.repository;

import com.z4greed.order.entity.InboxEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface InboxEventRepository extends JpaRepository<InboxEventEntity, String> {}
