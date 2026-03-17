package com.koushik.mq.api;

import com.koushik.mq.storage.Message;
import com.koushik.mq.storage.Partition;
import com.koushik.mq.storage.Topic;
import com.koushik.mq.storage.TopicManager;
import com.koushik.mq.replication.ReplicationManager;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Internal replication endpoints used by follower brokers.
 */
@RestController
@RequestMapping("/internal")
public class ReplicationController {

    private final TopicManager topicManager;
    private final ReplicationManager replicationManager;

    public ReplicationController(TopicManager topicManager, ReplicationManager replicationManager) {
        this.topicManager = topicManager;
        this.replicationManager = replicationManager;
    }

    /**
     * GET /internal/replicate/{topic}/{partition}?fromOffset=N
     * Returns batch of messages for follower replication.
     */
    @GetMapping("/replicate/{topic}/{partition}")
    public ResponseEntity<Map<String, Object>> replicate(
            @PathVariable String topic,
            @PathVariable int partition,
            @RequestParam(defaultValue = "0") long fromOffset,
            @RequestParam(defaultValue = "1000") int limit) throws IOException {

        Topic t = topicManager.getTopic(topic);
        if (t == null) {
            return ResponseEntity.notFound().build();
        }

        if (partition < 0 || partition >= t.getPartitionCount()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Invalid partition"));
        }

        Partition p = t.getPartition(partition);
        List<Message> messages = p.read(fromOffset, limit);

        List<Map<String, Object>> messageList = new ArrayList<>();
        for (Message msg : messages) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("offset", msg.offset());
            entry.put("timestamp", msg.timestamp());
            entry.put("key", msg.key() != null ? Base64.getEncoder().encodeToString(msg.key()) : null);
            entry.put("value", msg.value() != null ? Base64.getEncoder().encodeToString(msg.value()) : null);
            messageList.add(entry);
        }

        return ResponseEntity.ok(Map.of(
                "topic", topic,
                "partition", partition,
                "messages", messageList,
                "count", messages.size(),
                "latestOffset", p.getNextOffset()
        ));
    }

    /**
     * POST /internal/report-offset
     * Follower reports its replicated offset to the leader.
     */
    @PostMapping("/report-offset")
    public ResponseEntity<Map<String, Object>> reportOffset(
            @RequestBody Map<String, Object> body) {

        String topic = (String) body.get("topic");
        int partition = ((Number) body.get("partition")).intValue();
        String follower = (String) body.get("follower");
        long offset = ((Number) body.get("offset")).longValue();

        replicationManager.reportFollowerOffset(topic, partition, follower, offset);

        return ResponseEntity.ok(Map.of("status", "acknowledged"));
    }

    /**
     * GET /internal/status — broker cluster status
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> clusterStatus() {
        return ResponseEntity.ok(Map.of(
                "leaders", replicationManager.getPartitionLeaders(),
                "isr", replicationManager.getInSyncReplicas()
        ));
    }
}
