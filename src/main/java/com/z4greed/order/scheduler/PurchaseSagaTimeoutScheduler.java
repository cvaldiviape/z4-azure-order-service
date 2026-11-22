package com.z4greed.order.scheduler;

import com.z4greed.order.saga.PurchaseSagaTimeoutService;
import java.time.Duration;
import java.time.LocalDateTime;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class PurchaseSagaTimeoutScheduler {
    private final PurchaseSagaTimeoutService purchaseSagaTimeoutService;
    private final Duration timeout;
    private final int batchSize;

    public PurchaseSagaTimeoutScheduler(
            PurchaseSagaTimeoutService purchaseSagaTimeoutService,
            @Value("${app.saga.timeout-milliseconds}") long timeoutMilliseconds,
            @Value("${app.saga.timeout-batch-size}") int batchSize
    ) {
        this.purchaseSagaTimeoutService = purchaseSagaTimeoutService;
        this.timeout = Duration.ofMillis(timeoutMilliseconds);
        this.batchSize = batchSize;
    }

    // Las Sagas que pasan a TIMED_OUT quedan registradas en purchase_saga_histories.
    @Scheduled(
            fixedDelayString = "${app.saga.timeout-poll-interval-milliseconds}",
            initialDelayString = "${app.saga.timeout-initial-delay-milliseconds}")
    public void detectTimedOutSagas() {
        LocalDateTime cutoff = LocalDateTime.now().minus(this.timeout);
        this.purchaseSagaTimeoutService.timeoutStaleSagas(cutoff, this.batchSize);
    }
}
