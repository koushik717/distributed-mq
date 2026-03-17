package com.koushik.mq.storage;

import com.koushik.mq.config.MqConfig;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Manages all topics for this broker.
 * Auto-creates topics on first publish with default partition count.
 */
@Slf4j
@Component
public class TopicManager {

    private final ConcurrentMap<String, Topic> topics = new ConcurrentHashMap<>();
    private final MqConfig config;
    private final Path dataDir;

    public TopicManager(MqConfig config) {
        this.config = config;
        this.dataDir = Path.of(config.getDataDir());
        log.info("TopicManager initialized — dataDir={}, defaultPartitions={}",
                dataDir.toAbsolutePath(), config.getDefaultPartitionCount());
    }

    /**
     * Get or auto-create a topic with default partition count.
     */
    public Topic getOrCreateTopic(String name) {
        return topics.computeIfAbsent(name, n -> {
            try {
                log.info("Creating topic '{}' with {} partitions", n, config.getDefaultPartitionCount());
                return new Topic(n, config.getDefaultPartitionCount(), dataDir);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create topic: " + n, e);
            }
        });
    }

    /**
     * Create a topic with specified partition count.
     * Returns false if topic already exists.
     */
    public boolean createTopic(String name, int partitionCount) {
        if (topics.containsKey(name)) {
            return false;
        }
        try {
            Topic topic = new Topic(name, partitionCount, dataDir);
            Topic existing = topics.putIfAbsent(name, topic);
            if (existing != null) {
                topic.close();
                return false;
            }
            log.info("Created topic '{}' with {} partitions", name, partitionCount);
            return true;
        } catch (IOException e) {
            throw new RuntimeException("Failed to create topic: " + name, e);
        }
    }

    /**
     * Get a topic by name. Returns null if not found.
     */
    public Topic getTopic(String name) {
        return topics.get(name);
    }

    /**
     * Check if a topic exists.
     */
    public boolean topicExists(String name) {
        return topics.containsKey(name);
    }

    /**
     * Get all topic names.
     */
    public Collection<String> listTopics() {
        return topics.keySet();
    }

    @PreDestroy
    public void shutdown() throws IOException {
        log.info("Shutting down TopicManager — closing {} topics", topics.size());
        for (Topic topic : topics.values()) {
            topic.close();
        }
    }
}
