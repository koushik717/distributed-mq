package com.koushik.mq.consumer;

import com.koushik.mq.storage.Message;
import com.koushik.mq.storage.Partition;
import com.koushik.mq.storage.Topic;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A consumer group consuming from a specific topic.
 * <p>
 * Key semantics:
 * - Two consumers in same group = load balancing (each gets subset of partitions)
 * - Two consumers in different groups = fan-out (both get all messages)
 * - Offsets are persisted to disk per partition per group
 */
@Slf4j
@Getter
public class ConsumerGroup {

    private final String groupId;
    private final String topicName;
    private final Topic topic;
    private final Path offsetDir;

    // Offset tracking: partitionId → committed offset
    private final ConcurrentMap<Integer, Long> committedOffsets = new ConcurrentHashMap<>();

    // Partition assignment: consumerId → list of assigned partition IDs
    private final ConcurrentMap<String, List<Integer>> assignment = new ConcurrentHashMap<>();

    // Ordered list of consumer IDs for rebalancing
    private final List<String> consumers = Collections.synchronizedList(new ArrayList<>());

    public ConsumerGroup(String groupId, Topic topic, Path dataDir) throws IOException {
        this.groupId = groupId;
        this.topicName = topic.getName();
        this.topic = topic;
        this.offsetDir = dataDir.resolve("offsets").resolve(groupId).resolve(topicName);
        Files.createDirectories(offsetDir);

        // Load persisted offsets
        for (int i = 0; i < topic.getPartitionCount(); i++) {
            Path offsetFile = offsetDir.resolve("partition-" + i);
            if (Files.exists(offsetFile)) {
                String content = Files.readString(offsetFile).trim();
                committedOffsets.put(i, Long.parseLong(content));
            } else {
                committedOffsets.put(i, 0L);
            }
        }

        log.info("ConsumerGroup '{}' initialized for topic '{}', offsets={}", groupId, topicName, committedOffsets);
    }

    /**
     * Join a consumer to this group. Triggers partition rebalance.
     * Returns the consumer's partition assignment.
     */
    public synchronized List<Integer> join(String consumerId) {
        if (!consumers.contains(consumerId)) {
            consumers.add(consumerId);
            rebalance();
        }
        return assignment.getOrDefault(consumerId, Collections.emptyList());
    }

    /**
     * Remove a consumer from the group. Triggers rebalance.
     */
    public synchronized void leave(String consumerId) {
        consumers.remove(consumerId);
        assignment.remove(consumerId);
        if (!consumers.isEmpty()) {
            rebalance();
        }
    }

    /**
     * Range-based partition assignment strategy.
     * Distributes partitions evenly across consumers.
     */
    private void rebalance() {
        assignment.clear();
        if (consumers.isEmpty()) return;

        int partCount = topic.getPartitionCount();
        int consumerCount = consumers.size();

        // Sort consumers for deterministic assignment
        List<String> sorted = new ArrayList<>(consumers);
        Collections.sort(sorted);

        for (int i = 0; i < sorted.size(); i++) {
            assignment.put(sorted.get(i), new ArrayList<>());
        }

        // Range strategy: each consumer gets a contiguous range of partitions
        int partitionsPerConsumer = partCount / consumerCount;
        int remainder = partCount % consumerCount;

        int partitionIndex = 0;
        for (int i = 0; i < sorted.size(); i++) {
            int count = partitionsPerConsumer + (i < remainder ? 1 : 0);
            List<Integer> assigned = assignment.get(sorted.get(i));
            for (int j = 0; j < count; j++) {
                assigned.add(partitionIndex++);
            }
        }

        log.info("Rebalanced group '{}': {}", groupId, assignment);
    }

    /**
     * Poll messages for a specific consumer from their assigned partitions.
     */
    public List<Message> poll(String consumerId, int maxMessages) throws IOException {
        List<Integer> partitions = assignment.get(consumerId);
        if (partitions == null || partitions.isEmpty()) {
            return Collections.emptyList();
        }

        List<Message> messages = new ArrayList<>();
        int perPartition = Math.max(1, maxMessages / partitions.size());

        for (int partId : partitions) {
            long offset = committedOffsets.getOrDefault(partId, 0L);
            Partition partition = topic.getPartition(partId);
            List<Message> batch = partition.read(offset, perPartition);
            messages.addAll(batch);

            if (messages.size() >= maxMessages) break;
        }

        return messages;
    }

    /**
     * Commit offsets for a consumer. Persists to disk.
     */
    public void commit(String consumerId, Map<Integer, Long> offsets) throws IOException {
        List<Integer> assigned = assignment.get(consumerId);
        if (assigned == null) {
            throw new IllegalArgumentException("Consumer '" + consumerId + "' not in group '" + groupId + "'");
        }

        for (Map.Entry<Integer, Long> entry : offsets.entrySet()) {
            int partId = entry.getKey();
            long offset = entry.getValue();

            if (!assigned.contains(partId)) {
                throw new IllegalArgumentException(
                        "Consumer '" + consumerId + "' is not assigned partition " + partId);
            }

            committedOffsets.put(partId, offset);

            // Persist to disk
            Path offsetFile = offsetDir.resolve("partition-" + partId);
            Files.writeString(offsetFile, String.valueOf(offset));
        }

        log.debug("Committed offsets for consumer '{}' in group '{}': {}", consumerId, groupId, offsets);
    }

    /**
     * Get the committed offset for a partition.
     */
    public long getCommittedOffset(int partitionId) {
        return committedOffsets.getOrDefault(partitionId, 0L);
    }

    /**
     * Get consumer lag for a partition.
     * lag = latest offset in partition - committed offset for this group
     */
    public long getLag(int partitionId) {
        long latestOffset = topic.getPartition(partitionId).getNextOffset();
        long committed = committedOffsets.getOrDefault(partitionId, 0L);
        return latestOffset - committed;
    }

    /**
     * Get total lag across all partitions.
     */
    public long getTotalLag() {
        long totalLag = 0;
        for (int i = 0; i < topic.getPartitionCount(); i++) {
            totalLag += getLag(i);
        }
        return totalLag;
    }
}
