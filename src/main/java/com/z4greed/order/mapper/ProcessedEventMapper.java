package com.z4greed.order.mapper;

import com.z4greed.order.entity.ProcessedEventEntity;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import java.time.Instant;
import org.mapstruct.*;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING, imports = Instant.class)
public interface ProcessedEventMapper {
  @Named("ProcessedEventMapper.toEntity")
  @Mapping(target = "processedAt", expression = "java(Instant.now())")
  ProcessedEventEntity toEntity(EventEnvelopeDto eventEnvelopeDto);
}
