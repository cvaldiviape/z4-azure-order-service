package com.z4greed.order.strategy;

public interface EventStrategy<TEventType, TContext> {
  TEventType getEventType();
  void execute(TContext context);
}
