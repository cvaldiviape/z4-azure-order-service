package com.z4greed.order.mapper;

import com.z4greed.order.entity.ProcessedEventEntity;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import java.time.LocalDateTime;
import org.mapstruct.*;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING, imports = LocalDateTime.class)
public interface ProcessedEventMapper {
  @Named("ProcessedEventMapper.toEntity")
  @Mapping(target = "processedAt", expression = "java(LocalDateTime.now())")
  ProcessedEventEntity toEntity(EventEnvelopeDto eventEnvelopeDto);
}
