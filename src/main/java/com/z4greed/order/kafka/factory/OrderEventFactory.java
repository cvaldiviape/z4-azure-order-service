package com.z4greed.order.kafka.factory;

import com.z4greed.order.entity.OrderEntity;
import com.z4greed.order.enums.EventTypeEnum;
import com.z4greed.shared.kafka.event.IntegrationEventDto;
import com.z4greed.shared.kafka.event.IntegrationEventFactory;
import org.springframework.stereotype.Component;

// Adapta la fábrica genérica de z4-shared-kafka al pedido: el aggregateId es el id del pedido
// y el correlationId es el de la compra, para que todos sus eventos queden enlazados.
@Component
public class OrderEventFactory {
    private final IntegrationEventFactory integrationEventFactory;

    public OrderEventFactory(IntegrationEventFactory integrationEventFactory) {
        this.integrationEventFactory = integrationEventFactory;
    }

    public IntegrationEventDto build(EventTypeEnum eventTypeEnum, OrderEntity orderEntity) {
        String eventType = eventTypeEnum.getValue();
        String aggregateId = orderEntity.getId().toString();
        String correlationId = orderEntity.getCorrelationId();

        return this.integrationEventFactory.create(eventType, aggregateId, correlationId);
    }

    public IntegrationEventDto build(EventTypeEnum eventTypeEnum, OrderEntity orderEntity, String previousEventId, Object payload) {
        String eventType = eventTypeEnum.getValue();
        String aggregateId = orderEntity.getId().toString();
        String correlationId = orderEntity.getCorrelationId();

        return this.integrationEventFactory.create(eventType, aggregateId, correlationId, previousEventId, payload);
    }
}
