package com.z4greed.order.mapper;

import com.z4greed.order.dto.*;
import com.z4greed.order.entity.*;
import org.mapstruct.*;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface OrderMapper {
  @Named("OrderMapper.toEntity")
  @Mapping(target = "id", ignore = true)
  @Mapping(target = "updatedAt", ignore = true)
  @Mapping(target = "listItems", ignore = true)
  OrderEntity toEntity(OrderCreateDto dto);

  @Named("OrderMapper.toItemEntity")
  @Mapping(target = "id", ignore = true)
  OrderItemEntity toItemEntity(OrderItemCreateDto dto);

  @Named("OrderMapper.toPurchaseSagaEntity")
  @Mapping(target = "id", ignore = true)
  PurchaseSagaEntity toPurchaseSagaEntity(PurchaseSagaCreateDto dto);

  @Named("OrderMapper.toItemDto")
  ItemResponseDto toItemDto(OrderItemEntity entity);

  @IterableMapping(qualifiedByName = "OrderMapper.toItemDto")
  java.util.List<ItemResponseDto> toListItemDtos(java.util.List<OrderItemEntity> listEntities);

  @Named("OrderMapper.toDto")
  OrderResponseDto toDto(OrderEntity entity);
}
