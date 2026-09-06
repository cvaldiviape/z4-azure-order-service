package com.z4greed.order.scheduler;

import com.z4greed.order.saga.PurchaseSagaTimeoutService;
import java.time.Duration;
import java.time.LocalDateTime;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class PurchaseSagaTimeoutScheduler {
  private final PurchaseSagaTimeoutService purchaseSagaTimeoutService;
  private final long timeoutMilliseconds;
  private final int batchSize;

  public PurchaseSagaTimeoutScheduler(
      PurchaseSagaTimeoutService purchaseSagaTimeoutService,
      @Value("${app.saga.timeout-milliseconds}") long timeoutMilliseconds,
      @Value("${app.saga.timeout-batch-size}") int batchSize
  ) {
    this.purchaseSagaTimeoutService = purchaseSagaTimeoutService;
    this.timeoutMilliseconds = timeoutMilliseconds;
    this.batchSize = batchSize;
  }

  @Scheduled(
      fixedDelayString = "${app.saga.timeout-poll-interval-milliseconds}",
      initialDelayString = "${app.saga.timeout-initial-delay-milliseconds}")
  public void detectTimedOutSagas() {
    LocalDateTime cutoff = LocalDateTime.now().minus(Duration.ofMillis(this.timeoutMilliseconds));
    int timedOutSagas = this.purchaseSagaTimeoutService.timeoutStaleSagas(cutoff, this.batchSize);

    if (timedOutSagas > 0) {
      log.warn("action=saga_timeout_batch_processed timedOutSagas={} cutoff={}", timedOutSagas, cutoff);
    }
  }
}
