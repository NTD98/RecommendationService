package org.example.recommendationserviceapi.service;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.recommendationserviceapi.dto.UserInteraction;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class UserInteractionStreamProcessor {
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    public void process() throws Exception {
        // 1. Set up the Flink Execution Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable Checkpointing to guarantee Exactly-Once semantics and Fault Tolerance
        env.enableCheckpointing(10000); // Checkpoint every 10 seconds
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints");

        // 2. Configure the Kafka Source (The input pipe)
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9094,localhost:9095,localhost:9096")
                .setTopics("movie-clicks")
                .setGroupId("flink-recommender-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 3. Create the Data Stream
        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        stream
                // A. Map: Convert JSON String -> UserInteraction Object
                .map(value -> {
                    return jsonMapper.readValue(value, UserInteraction.class);
                })

                // B. KeyBy: Group streams by User ID
                .keyBy(UserInteraction::getUserId)

                // C. Window: Collect data for 10 seconds (Tumbling Window)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))

                // D. Process: The "Brain" logic
                .process(new GenreAggregator())

                // E. Sink: Print result to console (We will replace this with Redis later)
                .print();

        // 4. Execute the Job
        env.execute("Movie Recommendation Engine");
    }

    public static class GenreAggregator
            extends ProcessWindowFunction<UserInteraction, String, String, TimeWindow> {

        @Override
        public void process(String userId, Context context, Iterable<UserInteraction> elements, Collector<String> out) {

            // Map to store count: "Action" -> 2, "Comedy" -> 1
            Map<String, Integer> genreCounts = new HashMap<>();

            // 1. Loop through all clicks in this 10-second window
            for (UserInteraction event : elements) {
                String genre = event.getGenre();
                genreCounts.put(genre, genreCounts.getOrDefault(genre, 0) + 1);
            }

            // 2. Find the favorite genre
            String topGenre = "None";
            int maxCount = 0;

            for (Map.Entry<String, Integer> entry : genreCounts.entrySet()) {
                if (entry.getValue() > maxCount) {
                    maxCount = entry.getValue();
                    topGenre = entry.getKey();
                }
            }

            // 3. Output the result
            String result = "User: " + userId + " | Top Genre: " + topGenre + " | Window: " + context.window();
            out.collect(result);
        }
    }
}
