package org.example.recommendationserviceapi.controller;

import org.example.recommendationserviceapi.dto.UserInteraction;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ReceiveInteractionEventController {
    private static final String TOPIC_NAME = "movie-clicks";

    private final KafkaTemplate<String, UserInteraction> kafkaTemplate;

    public ReceiveInteractionEventController(KafkaTemplate<String, UserInteraction> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/click")
    public String trackClick(@RequestBody UserInteraction interaction) {
        // 1. Set timestamp if missing
        if (interaction.getTimestamp() == 0) {
            interaction.setTimestamp(System.currentTimeMillis());
        }

        // 2. Send to Kafka
        // KEY = userId (Ensures order), VALUE = The event object
        kafkaTemplate.send(TOPIC_NAME, interaction.getUserId(), interaction);

        return "Event sent to Kafka for user: " + interaction.getUserId();
    }
}
