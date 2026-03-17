package com.koushik.mq.api;

import com.koushik.mq.storage.Partition;
import com.koushik.mq.storage.Topic;
import com.koushik.mq.storage.TopicManager;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * Metadata API — topic information, partition status, topic management.
 */
@RestController
public class MetadataController {

    private final TopicManager topicManager;

    public MetadataController(TopicManager topicManager) {
        this.topicManager = topicManager;
    }

    /**
     * GET /topics — list all topics
     */
    @GetMapping("/topics")
    public ResponseEntity<Map<String, Object>> listTopics() {
        Collection<String> names = topicManager.listTopics();
        List<Map<String, Object>> topicList = new ArrayList<>();
        for (String name : names) {
            Topic t = topicManager.getTopic(name);
            if (t != null) {
                topicList.add(Map.of(
                        "name", name,
                        "partitions", t.getPartitionCount()
                ));
            }
        }
        return ResponseEntity.ok(Map.of("topics", topicList));
    }

    /**
     * POST /topics/{topic}?partitions=3 — create a topic with specified partitions
     */
    @PostMapping("/topics/{topic}")
    public ResponseEntity<Map<String, Object>> createTopic(
            @PathVariable String topic,
            @RequestParam(defaultValue = "3") int partitions) {

        if (partitions < 1 || partitions > 256) {
            return ResponseEntity.badRequest().body(
                    Map.of("error", "Partition count must be between 1 and 256"));
        }

        boolean created = topicManager.createTopic(topic, partitions);
        if (!created) {
            return ResponseEntity.status(409).body(
                    Map.of("error", "Topic '" + topic + "' already exists"));
        }

        return ResponseEntity.status(201).body(Map.of(
                "topic", topic,
                "partitions", partitions,
                "status", "created"
        ));
    }

    /**
     * GET /topics/{topic}/metadata — partition details and offsets
     */
    @GetMapping("/topics/{topic}/metadata")
    public ResponseEntity<Map<String, Object>> getMetadata(@PathVariable String topic) {
        Topic t = topicManager.getTopic(topic);
        if (t == null) {
            return ResponseEntity.notFound().build();
        }

        List<Map<String, Object>> partitionInfo = new ArrayList<>();
        for (Partition p : t.getPartitions()) {
            partitionInfo.add(Map.of(
                    "id", p.getPartitionId(),
                    "latestOffset", p.getLatestOffset(),
                    "nextOffset", p.getNextOffset()
            ));
        }

        return ResponseEntity.ok(Map.of(
                "topic", topic,
                "partitionCount", t.getPartitionCount(),
                "partitions", partitionInfo
        ));
    }
}
