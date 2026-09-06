package com.z4greed.order.service.impl;

import com.z4greed.order.entity.InboxEventEntity;
import com.z4greed.order.kafka.event.EventEnvelopeDto;
import com.z4greed.order.mapper.InboxEventMapper;
import com.z4greed.order.repository.InboxEventRepository;
import com.z4greed.order.service.InboxEventService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class InboxEventServiceImpl implements InboxEventService {
  private final InboxEventRepository inboxEventRepository;
  private final InboxEventMapper inboxEventMapper;

  public InboxEventServiceImpl(InboxEventRepository inboxEventRepository, InboxEventMapper inboxEventMapper) {
    this.inboxEventRepository = inboxEventRepository;
    this.inboxEventMapper = inboxEventMapper;
  }

  @Override @Transactional(readOnly = true)
  public boolean wasAlreadyProcessed(String eventId) { return this.inboxEventRepository.existsById(eventId); }

  @Override @Transactional
  public void register(EventEnvelopeDto event, String sourceTopic) {
    InboxEventEntity inboxEvent = this.inboxEventMapper.toEntity(event, sourceTopic);
    this.inboxEventRepository.save(inboxEvent);
  }

}
