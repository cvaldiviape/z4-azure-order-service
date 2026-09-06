package com.z4greed.order.mapper;

import com.z4greed.order.entity.InboxEventEntity;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import java.time.LocalDateTime;
import org.mapstruct.*;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING, imports = LocalDateTime.class)
public interface InboxEventMapper {
  @Named("InboxEventMapper.toEntity")
  @Mapping(target = "processedAt", expression = "java(LocalDateTime.now())")
  InboxEventEntity toEntity(EventEnvelopeDto eventEnvelopeDto, String sourceTopic);
}
