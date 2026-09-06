package com.z4greed.order.scheduler;

import com.z4greed.order.entity.OutboxEventEntity;
import com.z4greed.order.kafka.producer.OrderEventProducer;
import com.z4greed.order.service.OutboxEventService;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class OutboxRelayScheduler {
  private final OutboxEventService outboxEventService;
  private final OrderEventProducer orderEventProducer;
  private final int batchSize;

  public OutboxRelayScheduler(
          OutboxEventService outboxEventService,
          OrderEventProducer orderEventProducer,
          @Value("${app.outbox.batch-size}") int batchSize
  ) {
    this.outboxEventService = outboxEventService;
    this.orderEventProducer = orderEventProducer;
    this.batchSize = batchSize;
  }

  @Scheduled(fixedDelayString = "${app.outbox.poll-interval-milliseconds}", initialDelayString = "${app.outbox.initial-delay-milliseconds}")
  public void publishPendingEvents() {
    List<OutboxEventEntity> listPendingEvents = this.outboxEventService.findPending(this.batchSize);

    for (OutboxEventEntity outboxEvent : listPendingEvents) {
      String eventId = outboxEvent.getEventId();

      try {
        this.orderEventProducer.publishAndWait(outboxEvent);
        this.outboxEventService.markAsPublished(eventId);
      } catch (RuntimeException exception) {
        this.printLogError(outboxEvent, exception, eventId);
      }
    }
  }

  private void printLogError(OutboxEventEntity outboxEvent, RuntimeException exception, String eventId) {
    // Ante timeout o error de Kafka no se marca PUBLISHED. El registro continúa PENDING para que un ciclo posterior del scheduler vuelva a intentarlo.
    String errorMessage = this.limitErrorMessage(exception.getMessage());
    this.outboxEventService.registerFailedAttempt(eventId, errorMessage);

    String eventType = outboxEvent.getEventType();
    String correlationId = outboxEvent.getCorrelationId();
    String aggregateId = outboxEvent.getAggregateId();
    int i = outboxEvent.getAttempts() + 1;
    log.error("action=outbox_publish_failed eventType={} eventId={} correlationId={} orderId={} attempts={}", eventType, eventId, correlationId, aggregateId, i, exception);
  }

  private String limitErrorMessage(String errorMessage) {
    if (errorMessage == null) {
      return "Unknown error";
    }
    return errorMessage.substring(0, Math.min(errorMessage.length(), 2000));
  }

}