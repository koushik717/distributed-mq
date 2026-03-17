package com.koushik.mq.api;

import com.koushik.mq.consumer.ConsumerGroup;
import com.koushik.mq.consumer.ConsumerGroupManager;
import com.koushik.mq.storage.Message;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Consumer Group API — join, poll, commit, status.
 */
@RestController
@RequestMapping("/consumer-groups/{groupId}")
public class ConsumerGroupController {

    private final ConsumerGroupManager groupManager;

    public ConsumerGroupController(ConsumerGroupManager groupManager) {
        this.groupManager = groupManager;
    }

    /**
     * POST /consumer-groups/{groupId}/join?topic=orders
     * Registers a consumer in the group and returns partition assignment.
     */
    @PostMapping("/join")
    public ResponseEntity<Map<String, Object>> join(
            @PathVariable String groupId,
            @RequestParam String topic,
            @RequestParam(required = false) String consumerId) {

        if (consumerId == null || consumerId.isEmpty()) {
            consumerId = "consumer-" + UUID.randomUUID().toString().substring(0, 8);
        }

        ConsumerGroup group = groupManager.getOrCreateGroup(groupId, topic);
        List<Integer> assignment = group.join(consumerId);

        return ResponseEntity.ok(Map.of(
                "groupId", groupId,
                "topic", topic,
                "consumerId", consumerId,
                "assignment", assignment
        ));
    }

    /**
     * GET /consumer-groups/{groupId}/poll?topic=orders&consumerId=c1&limit=100
     * Returns messages from the consumer's assigned partitions.
     */
    @GetMapping("/poll")
    public ResponseEntity<Map<String, Object>> poll(
            @PathVariable String groupId,
            @RequestParam String topic,
            @RequestParam String consumerId,
            @RequestParam(defaultValue = "100") int limit) throws IOException {

        ConsumerGroup group = groupManager.getGroup(groupId, topic);
        if (group == null) {
            return ResponseEntity.notFound().build();
        }

        List<Message> messages = group.poll(consumerId, limit);

        List<Map<String, Object>> messageList = new ArrayList<>();
        for (Message msg : messages) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("offset", msg.offset());
            entry.put("partition", findPartitionForOffset(group, msg));
            entry.put("timestamp", msg.timestamp());
            entry.put("key", msg.key() != null ? new String(msg.key(), StandardCharsets.UTF_8) : null);
            entry.put("value", new String(msg.value(), StandardCharsets.UTF_8));
            messageList.add(entry);
        }

        return ResponseEntity.ok(Map.of(
                "groupId", groupId,
                "topic", topic,
                "consumerId", consumerId,
                "messages", messageList,
                "count", messages.size()
        ));
    }

    /**
     * POST /consumer-groups/{groupId}/commit
     * Body: {"consumerId":"c1", "topic":"orders", "offsets":{"0":150, "1":200}}
     */
    @PostMapping("/commit")
    public ResponseEntity<Map<String, Object>> commit(
            @PathVariable String groupId,
            @RequestBody Map<String, Object> body) throws IOException {

        String consumerId = (String) body.get("consumerId");
        String topic = (String) body.get("topic");

        @SuppressWarnings("unchecked")
        Map<String, Object> rawOffsets = (Map<String, Object>) body.get("offsets");

        if (consumerId == null || topic == null || rawOffsets == null) {
            return ResponseEntity.badRequest().body(
                    Map.of("error", "consumerId, topic, and offsets are required"));
        }

        ConsumerGroup group = groupManager.getGroup(groupId, topic);
        if (group == null) {
            return ResponseEntity.notFound().build();
        }

        // Parse offsets (JSON keys are strings)
        Map<Integer, Long> offsets = new HashMap<>();
        for (Map.Entry<String, Object> entry : rawOffsets.entrySet()) {
            offsets.put(Integer.parseInt(entry.getKey()),
                    ((Number) entry.getValue()).longValue());
        }

        group.commit(consumerId, offsets);

        return ResponseEntity.ok(Map.of(
                "status", "committed",
                "groupId", groupId,
                "consumerId", consumerId,
                "offsets", offsets
        ));
    }

    /**
     * GET /consumer-groups/{groupId}/status?topic=orders
     * Returns group status including assignments and lag.
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status(
            @PathVariable String groupId,
            @RequestParam String topic) {

        ConsumerGroup group = groupManager.getGroup(groupId, topic);
        if (group == null) {
            return ResponseEntity.notFound().build();
        }

        // Build lag info per partition
        List<Map<String, Object>> partitionLag = new ArrayList<>();
        for (int i = 0; i < group.getTopic().getPartitionCount(); i++) {
            partitionLag.add(Map.of(
                    "partition", i,
                    "committedOffset", group.getCommittedOffset(i),
                    "latestOffset", group.getTopic().getPartition(i).getNextOffset(),
                    "lag", group.getLag(i)
            ));
        }

        return ResponseEntity.ok(Map.of(
                "groupId", groupId,
                "topic", topic,
                "consumers", group.getAssignment(),
                "totalLag", group.getTotalLag(),
                "partitions", partitionLag
        ));
    }

    private int findPartitionForOffset(ConsumerGroup group, Message msg) {
        // Messages carry their offset; we can use the assignment to determine partition
        // For simplicity, return -1 as the partition info is in the consumer context
        return -1;
    }
}
