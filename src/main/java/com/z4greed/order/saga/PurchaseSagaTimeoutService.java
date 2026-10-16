package com.z4greed.order.saga;

import java.time.LocalDateTime;

public interface PurchaseSagaTimeoutService {
  int timeoutStaleSagas(LocalDateTime cutoff, int batchSize);
}
