package com.z4greed.order.mapper;

import com.z4greed.order.dto.*;
import com.z4greed.order.entity.*;
import org.mapstruct.*;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface OrderMapper {
  @Named("OrderMapper.toItemDto")
  ItemResponseDto toItemDto(OrderItemEntity entity);

  @IterableMapping(qualifiedByName = "OrderMapper.toItemDto")
  java.util.List<ItemResponseDto> toListItemDtos(java.util.List<OrderItemEntity> listEntities);

  @Named("OrderMapper.toDto")
  OrderResponseDto toDto(OrderEntity entity);
}
