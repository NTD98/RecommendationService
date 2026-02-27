package org.example.recommendationserviceapi.controller;

import org.example.recommendationserviceapi.dto.UserInteraction;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ReceiveInteractionEventController {

    private final ApplicationEventPublisher events;

    public ReceiveInteractionEventController(ApplicationEventPublisher events) {
        this.events = events;
    }

    @PostMapping("/click")
    @Transactional
    public String trackClick(@RequestBody UserInteraction interaction) {
        // 1. Set timestamp if missing
        if (interaction.getTimestamp() == 0) {
            interaction.setTimestamp(System.currentTimeMillis());
        }

        // 2. Publish event (Spring Modulith will handle the outbox pattern)
        events.publishEvent(interaction);

        return "Event sent to Kafka for user: " + interaction.getUserId();
    }
}
