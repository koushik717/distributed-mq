package com.koushik.mq.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Topic — partition selection and key-based routing.
 */
class TopicTest {

    @TempDir
    Path tempDir;

    @Test
    void same_key_always_routes_to_same_partition() throws IOException {
        Topic topic = new Topic("orders", 6, tempDir);

        byte[] key = "user-123".getBytes(StandardCharsets.UTF_8);
        int partition1 = topic.selectPartition(key);
        int partition2 = topic.selectPartition(key);
        int partition3 = topic.selectPartition(key);

        assertEquals(partition1, partition2);
        assertEquals(partition2, partition3);
        topic.close();
    }

    @Test
    void null_key_uses_round_robin() throws IOException {
        Topic topic = new Topic("events", 3, tempDir);

        Set<Integer> seen = new HashSet<>();
        for (int i = 0; i < 9; i++) {
            seen.add(topic.selectPartition(null));
        }

        // Round-robin should hit all 3 partitions
        assertEquals(3, seen.size());
        topic.close();
    }

    @Test
    void different_keys_distribute_across_partitions() throws IOException {
        Topic topic = new Topic("users", 4, tempDir);

        Set<Integer> partitions = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            byte[] key = ("user-" + i).getBytes(StandardCharsets.UTF_8);
            partitions.add(topic.selectPartition(key));
        }

        // With 100 different keys, we expect to hit all 4 partitions
        assertEquals(4, partitions.size());
        topic.close();
    }

    @Test
    void partition_count_matches_creation() throws IOException {
        Topic topic = new Topic("test", 5, tempDir);
        assertEquals(5, topic.getPartitionCount());
        assertEquals(5, topic.getPartitions().size());
        topic.close();
    }

    @Test
    void invalid_partition_id_throws() throws IOException {
        Topic topic = new Topic("test", 3, tempDir);
        assertThrows(IllegalArgumentException.class, () -> topic.getPartition(-1));
        assertThrows(IllegalArgumentException.class, () -> topic.getPartition(3));
        topic.close();
    }
}
