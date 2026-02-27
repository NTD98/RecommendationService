package org.example.recommendationserviceapi;

import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableAsync
@EnableScheduling
@EnableSchedulerLock(defaultLockAtMostFor = "10m")
@SpringBootApplication
public class RecommendationServiceApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(RecommendationServiceApiApplication.class, args);
    }

}
