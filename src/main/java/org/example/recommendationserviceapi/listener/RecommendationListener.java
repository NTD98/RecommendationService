package org.example.recommendationserviceapi.listener;

import org.example.recommendationserviceapi.service.UserInteractionStreamProcessor;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class RecommendationListener {

    private final UserInteractionStreamProcessor streamProcessor;

    public RecommendationListener(UserInteractionStreamProcessor streamProcessor) {
        this.streamProcessor = streamProcessor;
    }
    
    @EventListener(ApplicationReadyEvent.class)
    public void listenUserInteractions() {
        try {
            streamProcessor.process();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
