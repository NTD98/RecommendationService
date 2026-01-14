package org.example.recommendationserviceapi.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data // Generates Getters, Setters, ToString
@AllArgsConstructor
@NoArgsConstructor
public class UserInteraction {
    private String userId;
    private String movieId;
    private String genre;
    private String actionType; // e.g., "CLICK"
    private long timestamp;
}
