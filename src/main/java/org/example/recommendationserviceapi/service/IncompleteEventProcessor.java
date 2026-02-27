package org.example.recommendationserviceapi.service;

import org.springframework.modulith.events.IncompleteEventPublications;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Service
public class IncompleteEventProcessor {

    private final IncompleteEventPublications incompleteEvents;

    public IncompleteEventProcessor(IncompleteEventPublications incompleteEvents) {
        this.incompleteEvents = incompleteEvents;
    }

    @Scheduled(fixedRate = 1, timeUnit = TimeUnit.MINUTES)
    public void reprocessIncompleteEvents() {
        incompleteEvents.resubmitIncompletePublicationsOlderThan(Duration.ofMinutes(1));
    }
}
