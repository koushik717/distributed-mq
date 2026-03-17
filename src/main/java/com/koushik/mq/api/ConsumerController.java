package com.koushik.mq.api;

import com.koushik.mq.metrics.MqMetrics;
import com.koushik.mq.storage.Message;
import com.koushik.mq.storage.Partition;
import com.koushik.mq.storage.Topic;
import com.koushik.mq.storage.TopicManager;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Consumer API — poll messages from topics.
 * <p>
 * GET /topics/{topic}/messages?offset=0&limit=100&partition=0
 */
@RestController
@RequestMapping("/topics/{topic}/messages")
public class ConsumerController {

    private final TopicManager topicManager;
    private final MqMetrics metrics;

    public ConsumerController(TopicManager topicManager, MqMetrics metrics) {
        this.topicManager = topicManager;
        this.metrics = metrics;
    }

    @GetMapping
    public ResponseEntity<Map<String, Object>> poll(
            @PathVariable String topic,
            @RequestParam(defaultValue = "0") long offset,
            @RequestParam(defaultValue = "100") int limit,
            @RequestParam(defaultValue = "0") int partition) throws IOException {

        Topic t = topicManager.getTopic(topic);
        if (t == null) {
            return ResponseEntity.notFound().build();
        }

        if (partition < 0 || partition >= t.getPartitionCount()) {
            return ResponseEntity.badRequest().body(
                    Map.of("error", "Invalid partition: " + partition +
                            ". Topic has " + t.getPartitionCount() + " partitions."));
        }

        Partition p = t.getPartition(partition);
        List<Message> messages = p.read(offset, limit);

        // Record metrics
        metrics.recordConsume(topic, partition, messages.size());

        // Convert messages to JSON-friendly format
        List<Map<String, Object>> messageList = new ArrayList<>();
        for (Message msg : messages) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("offset", msg.offset());
            entry.put("timestamp", msg.timestamp());
            entry.put("key", msg.key() != null ? new String(msg.key(), StandardCharsets.UTF_8) : null);
            entry.put("value", new String(msg.value(), StandardCharsets.UTF_8));
            messageList.add(entry);
        }

        long nextOffset = messages.isEmpty()
                ? offset
                : messages.get(messages.size() - 1).offset() + 1;

        return ResponseEntity.ok(Map.of(
                "topic", topic,
                "partition", partition,
                "messages", messageList,
                "count", messages.size(),
                "nextOffset", nextOffset
        ));
    }
}
