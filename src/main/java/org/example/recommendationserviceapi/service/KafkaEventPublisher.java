package org.example.recommendationserviceapi.service;

import org.example.recommendationserviceapi.dto.UserInteraction;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

@Service
public class KafkaEventPublisher {

    private static final String TOPIC_NAME = "movie-clicks";

    private final KafkaTemplate<String, UserInteraction> kafkaTemplate;

    public KafkaEventPublisher(KafkaTemplate<String, UserInteraction> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @ApplicationModuleListener
    public void handleUserInteraction(UserInteraction interaction) throws ExecutionException, InterruptedException {
        kafkaTemplate.send(TOPIC_NAME, interaction.getUserId(), interaction).get();
    }
}
