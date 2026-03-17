package com.koushik.mq.consumer;

import com.koushik.mq.storage.Message;
import com.koushik.mq.storage.Partition;
import com.koushik.mq.storage.Topic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ConsumerGroup — partition assignment, poll, commit, and fan-out.
 */
class ConsumerGroupTest {

    @TempDir
    Path tempDir;

    private Topic topic;

    @BeforeEach
    void setUp() throws IOException {
        topic = new Topic("orders", 4, tempDir);

        // Pre-populate each partition with some messages
        for (int p = 0; p < 4; p++) {
            Partition partition = topic.getPartition(p);
            for (int i = 0; i < 10; i++) {
                partition.append(
                        ("key-" + p + "-" + i).getBytes(StandardCharsets.UTF_8),
                        ("value-" + p + "-" + i).getBytes(StandardCharsets.UTF_8)
                );
            }
        }
    }

    @Test
    void single_consumer_gets_all_partitions() throws IOException {
        ConsumerGroup group = new ConsumerGroup("group-1", topic, tempDir);
        List<Integer> assignment = group.join("c1");

        assertEquals(4, assignment.size());
        assertTrue(assignment.containsAll(List.of(0, 1, 2, 3)));
    }

    @Test
    void two_consumers_split_partitions() throws IOException {
        ConsumerGroup group = new ConsumerGroup("group-2", topic, tempDir);
        group.join("c1");
        List<Integer> c2Assignment = group.join("c2");

        // Each consumer should get 2 partitions
        Map<String, List<Integer>> assignments = group.getAssignment();
        assertEquals(2, assignments.get("c1").size());
        assertEquals(2, assignments.get("c2").size());

        // No overlap
        List<Integer> all = new java.util.ArrayList<>(assignments.get("c1"));
        all.addAll(assignments.get("c2"));
        assertEquals(4, all.stream().distinct().count());
    }

    @Test
    void poll_returns_messages_from_assigned_partitions() throws IOException {
        ConsumerGroup group = new ConsumerGroup("group-3", topic, tempDir);
        group.join("c1");

        List<Message> messages = group.poll("c1", 20);
        assertFalse(messages.isEmpty());
    }

    @Test
    void commit_advances_offset() throws IOException {
        ConsumerGroup group = new ConsumerGroup("group-4", topic, tempDir);
        group.join("c1");

        assertEquals(0, group.getCommittedOffset(0));

        group.commit("c1", Map.of(0, 5L, 1, 3L));

        assertEquals(5, group.getCommittedOffset(0));
        assertEquals(3, group.getCommittedOffset(1));
    }

    @Test
    void committed_offsets_survive_restart() throws IOException {
        ConsumerGroup group1 = new ConsumerGroup("group-5", topic, tempDir);
        group1.join("c1");
        group1.commit("c1", Map.of(0, 7L, 1, 4L));

        // Create a new instance — should reload offsets from disk
        ConsumerGroup group2 = new ConsumerGroup("group-5", topic, tempDir);
        assertEquals(7, group2.getCommittedOffset(0));
        assertEquals(4, group2.getCommittedOffset(1));
    }

    @Test
    void independent_groups_get_independent_offsets() throws IOException {
        ConsumerGroup groupA = new ConsumerGroup("group-a", topic, tempDir);
        ConsumerGroup groupB = new ConsumerGroup("group-b", topic, tempDir);

        groupA.join("c1");
        groupB.join("c2");

        groupA.commit("c1", Map.of(0, 8L));
        groupB.commit("c2", Map.of(0, 3L));

        // Each group tracks its own offsets
        assertEquals(8, groupA.getCommittedOffset(0));
        assertEquals(3, groupB.getCommittedOffset(0));
    }

    @Test
    void consumer_lag_calculation() throws IOException {
        ConsumerGroup group = new ConsumerGroup("group-lag", topic, tempDir);
        group.join("c1");

        // Each partition has 10 messages, committed offset is 0
        assertEquals(10, group.getLag(0));
        assertEquals(40, group.getTotalLag());

        group.commit("c1", Map.of(0, 5L));
        assertEquals(5, group.getLag(0));
        assertEquals(35, group.getTotalLag());
    }

    @Test
    void rebalance_on_consumer_leave() throws IOException {
        ConsumerGroup group = new ConsumerGroup("group-rebal", topic, tempDir);
        group.join("c1");
        group.join("c2");

        Map<String, List<Integer>> before = group.getAssignment();
        assertEquals(2, before.get("c1").size());
        assertEquals(2, before.get("c2").size());

        group.leave("c2");

        // c1 should now have all 4 partitions
        Map<String, List<Integer>> after = group.getAssignment();
        assertNull(after.get("c2"));
        assertEquals(4, after.get("c1").size());
    }
}
