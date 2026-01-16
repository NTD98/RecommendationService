package org.example.recommendationserviceapi.service;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
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
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Service
public class UserInteractionStreamProcessor {
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    public void process() throws Exception {
        // 1. Set up the Flink Execution Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable Checkpointing to guarantee Exactly-Once semantics and Fault Tolerance
        env.enableCheckpointing(60000); // Checkpoint every 60 seconds
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

                // C. Window: Collect data for 30 days (Tumbling Window)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))

                // D. Process: The "Brain" logic (Calculates and writes to Redis directly)
                .process(new GenreAggregator());

        // 4. Execute the Job
        env.execute("Movie Recommendation Engine");
    }

    public static class GenreAggregator
            extends ProcessWindowFunction<UserInteraction, Void, String, TimeWindow> {

        private transient JedisCluster jedisCluster;

        @Override
        public void open(Configuration parameters) throws Exception {
            Set<HostAndPort> jedisClusterNodes = new HashSet<>();
            jedisClusterNodes.add(new HostAndPort("localhost", 6379));
            jedisClusterNodes.add(new HostAndPort("localhost", 6380));
            jedisClusterNodes.add(new HostAndPort("localhost", 6381));
            jedisCluster = new JedisCluster(jedisClusterNodes);
        }

        @Override
        public void close() throws Exception {
            if (jedisCluster != null) {
                jedisCluster.close();
            }
        }

        @Override
        public void process(String userId, Context context, Iterable<UserInteraction> elements, Collector<Void> out) {

            // 1. Calculate counts for the current window
            Map<String, Double> currentWindowCounts = new HashMap<>();
            for (UserInteraction event : elements) {
                String genre = event.getGenre();
                currentWindowCounts.put(genre, currentWindowCounts.getOrDefault(genre, 0.0) + 1.0);
            }

            String redisKey = "user_genres:" + userId;

            try {
                // 2. Check if key exists in Redis
                if (jedisCluster.exists(redisKey)) {
                    // If exist: Recompute new value with result from Redis (Update existing scores)
                    for (Map.Entry<String, Double> entry : currentWindowCounts.entrySet()) {
                        jedisCluster.zincrby(redisKey, entry.getValue(), entry.getKey());
                    }
                } else {
                    // If not: Assume user doesn't have computed value, set current window as latest
                    jedisCluster.zadd(redisKey, currentWindowCounts);
                }
            } catch (Exception e) {
                System.err.println("Failed to write to Redis for user " + userId + ": " + e.getMessage());
            }
        }
    }
}
