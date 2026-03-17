package com.koushik.mq.api;

import com.koushik.mq.metrics.MqMetrics;
import com.koushik.mq.storage.Partition;
import com.koushik.mq.storage.Topic;
import com.koushik.mq.storage.TopicManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Producer API — publish messages to topics.
 * <p>
 * POST /topics/{topic}/messages
 */
@Slf4j
@RestController
@RequestMapping("/topics/{topic}/messages")
public class ProducerController {

    private final TopicManager topicManager;
    private final MqMetrics metrics;

    public ProducerController(TopicManager topicManager, MqMetrics metrics) {
        this.topicManager = topicManager;
        this.metrics = metrics;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> publish(
            @PathVariable String topic,
            @RequestBody Map<String, String> body) throws IOException {

        String keyStr = body.get("key");
        String valueStr = body.get("value");

        if (valueStr == null || valueStr.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "value is required"));
        }

        byte[] key = (keyStr != null && !keyStr.isEmpty())
                ? keyStr.getBytes(StandardCharsets.UTF_8)
                : null;
        byte[] value = valueStr.getBytes(StandardCharsets.UTF_8);

        // Get or auto-create topic
        Topic t = topicManager.getOrCreateTopic(topic);

        // Select partition
        int partitionId = t.selectPartition(key);
        Partition partition = t.getPartition(partitionId);

        // Append to log
        long offset = partition.append(key, value);

        // Record metrics
        metrics.recordProduce(topic, partitionId);
        metrics.recordBytesWritten(topic, value.length + (key != null ? key.length : 0));

        log.debug("Published to topic={} partition={} offset={}", topic, partitionId, offset);

        return ResponseEntity.ok(Map.of(
                "topic", topic,
                "partition", partitionId,
                "offset", offset,
                "timestamp", System.currentTimeMillis()
        ));
    }
}
