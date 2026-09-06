package com.z4greed.order.strategy;

public interface EventStrategyRegistry<TEventType, TContext> {
  EventStrategy<TEventType, TContext> find(TEventType eventType);
}
