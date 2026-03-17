package com.koushik.mq.consumer;

import com.koushik.mq.config.MqConfig;
import com.koushik.mq.storage.Topic;
import com.koushik.mq.storage.TopicManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Registry for consumer groups.
 */
@Slf4j
@Component
public class ConsumerGroupManager {

    private final ConcurrentMap<String, ConsumerGroup> groups = new ConcurrentHashMap<>();
    private final TopicManager topicManager;
    private final Path dataDir;

    public ConsumerGroupManager(TopicManager topicManager, MqConfig config) {
        this.topicManager = topicManager;
        this.dataDir = Path.of(config.getDataDir());
    }

    /**
     * Get or create a consumer group for a topic.
     */
    public ConsumerGroup getOrCreateGroup(String groupId, String topicName) {
        String key = groupId + ":" + topicName;
        return groups.computeIfAbsent(key, k -> {
            Topic topic = topicManager.getOrCreateTopic(topicName);
            try {
                return new ConsumerGroup(groupId, topic, dataDir);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create consumer group: " + groupId, e);
            }
        });
    }

    /**
     * Get a consumer group. Returns null if not found.
     */
    public ConsumerGroup getGroup(String groupId, String topicName) {
        return groups.get(groupId + ":" + topicName);
    }

    /**
     * List all consumer group IDs.
     */
    public Collection<String> listGroups() {
        return groups.keySet();
    }
}
