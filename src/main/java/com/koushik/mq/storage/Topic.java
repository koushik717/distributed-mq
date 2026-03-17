package com.koushik.mq.storage;

import com.koushik.mq.util.MurmurHash3;
import lombok.Getter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A topic contains one or more partitions.
 * Messages are assigned to partitions by key hash (deterministic) or round-robin.
 */
@Getter
public class Topic {

    private final String name;
    private final int partitionCount;
    private final List<Partition> partitions;
    private final AtomicInteger roundRobin = new AtomicInteger(0);

    public Topic(String name, int partitionCount, Path dataDir) throws IOException {
        this.name = name;
        this.partitionCount = partitionCount;

        Path topicDir = dataDir.resolve("topics").resolve(name);
        List<Partition> parts = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            parts.add(new Partition(i, topicDir));
        }
        this.partitions = Collections.unmodifiableList(parts);
    }

    /**
     * Select which partition a message should go to.
     * - If key is provided: murmurHash(key) % partitionCount → same key always goes to same partition
     * - If key is null: round-robin across partitions
     */
    public int selectPartition(byte[] key) {
        if (key == null || key.length == 0) {
            return Math.abs(roundRobin.getAndIncrement()) % partitionCount;
        }
        return Math.abs(MurmurHash3.hash32(key)) % partitionCount;
    }

    /**
     * Get a specific partition by ID.
     */
    public Partition getPartition(int partitionId) {
        if (partitionId < 0 || partitionId >= partitions.size()) {
            throw new IllegalArgumentException("Invalid partition ID: " + partitionId);
        }
        return partitions.get(partitionId);
    }

    /**
     * Close all partitions.
     */
    public void close() throws IOException {
        for (Partition partition : partitions) {
            partition.close();
        }
    }
}
