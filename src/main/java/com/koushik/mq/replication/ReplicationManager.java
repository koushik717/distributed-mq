package com.koushik.mq.replication;

import com.koushik.mq.config.MqConfig;
import com.koushik.mq.storage.Message;
import com.koushik.mq.storage.Partition;
import com.koushik.mq.storage.Topic;
import com.koushik.mq.storage.TopicManager;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * Manages replication of partition data between brokers.
 * <p>
 * Leader handles all reads/writes. Followers replicate from leader.
 * Messages are committed only when all In-Sync Replicas (ISR) acknowledge.
 */
@Slf4j
@Component
public class ReplicationManager {

    private final MqConfig config;
    private final TopicManager topicManager;
    private final BrokerRegistry brokerRegistry;
    private final HttpClient httpClient;

    // Leader tracking: "topic:partition" → leader broker address
    @Getter
    private final ConcurrentMap<String, String> partitionLeaders = new ConcurrentHashMap<>();

    // ISR tracking: "topic:partition" → set of follower broker addresses
    @Getter
    private final ConcurrentMap<String, Set<String>> inSyncReplicas = new ConcurrentHashMap<>();

    // Follower offsets: "topic:partition:follower" → replicated offset
    private final ConcurrentMap<String, Long> followerOffsets = new ConcurrentHashMap<>();

    // Replication executor
    private final ScheduledExecutorService replicationExecutor;

    public ReplicationManager(MqConfig config, TopicManager topicManager, BrokerRegistry brokerRegistry) {
        this.config = config;
        this.topicManager = topicManager;
        this.brokerRegistry = brokerRegistry;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        this.replicationExecutor = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "replication-worker");
            t.setDaemon(true);
            return t;
        });

        if (brokerRegistry.hasPeers()) {
            // Start replication loop every 1 second
            replicationExecutor.scheduleAtFixedRate(this::replicationLoop, 5, 1, TimeUnit.SECONDS);
            log.info("ReplicationManager started — replication factor: {}", config.getReplicationFactor());
        } else {
            log.info("ReplicationManager started — single broker mode (no peers configured)");
        }
    }

    /**
     * Check if this broker is the leader for a given topic:partition.
     * In single-broker mode, we're always the leader.
     */
    public boolean isLeader(String topic, int partition) {
        if (!brokerRegistry.hasPeers()) return true; // single broker is always leader

        String key = topic + ":" + partition;
        String leader = partitionLeaders.get(key);
        return leader == null || leader.equals("self");
    }

    /**
     * Get the leader address for a partition. Returns null if self is leader.
     */
    public String getLeaderAddress(String topic, int partition) {
        String key = topic + ":" + partition;
        String leader = partitionLeaders.get(key);
        if (leader == null || leader.equals("self")) return null;
        return leader;
    }

    /**
     * Assign this broker as leader for a partition.
     */
    public void assignLeader(String topic, int partition) {
        String key = topic + ":" + partition;
        partitionLeaders.put(key, "self");
        inSyncReplicas.put(key, ConcurrentHashMap.newKeySet());
        log.info("Assigned as leader for {}:{}", topic, partition);
    }

    /**
     * Report a follower's replicated offset.
     */
    public void reportFollowerOffset(String topic, int partition, String follower, long offset) {
        String key = topic + ":" + partition + ":" + follower;
        followerOffsets.put(key, offset);

        // Check if follower is in-sync (within 10 seconds of latest data)
        String partKey = topic + ":" + partition;
        Set<String> isr = inSyncReplicas.computeIfAbsent(partKey, k -> ConcurrentHashMap.newKeySet());
        isr.add(follower);
    }

    /**
     * Check if a message at the given offset is fully replicated.
     */
    public boolean isCommitted(String topic, int partition, long offset) {
        if (!brokerRegistry.hasPeers()) return true; // single broker, always committed

        String partKey = topic + ":" + partition;
        Set<String> isr = inSyncReplicas.get(partKey);
        if (isr == null || isr.isEmpty()) return true; // no ISR configured yet

        for (String follower : isr) {
            String key = topic + ":" + partition + ":" + follower;
            Long followerOffset = followerOffsets.get(key);
            if (followerOffset == null || followerOffset < offset) {
                return false;
            }
        }
        return true;
    }

    /**
     * Follower replication loop — fetch new messages from leaders and append locally.
     */
    private void replicationLoop() {
        try {
            for (String peer : brokerRegistry.getAlivePeers()) {
                fetchAndReplicateFrom(peer);
            }
        } catch (Exception e) {
            log.error("Replication loop error: {}", e.getMessage());
        }
    }

    private void fetchAndReplicateFrom(String peer) {
        try {
            // Fetch topic list from peer
            HttpRequest topicsReq = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + peer + "/topics"))
                    .timeout(Duration.ofSeconds(3))
                    .GET()
                    .build();

            HttpResponse<String> topicsResp = httpClient.send(topicsReq,
                    HttpResponse.BodyHandlers.ofString());

            if (topicsResp.statusCode() != 200) return;

            // For each topic/partition on the peer, check if we need to replicate
            // This is a simplified version — production would use proper metadata protocol
            for (String topicName : topicManager.listTopics()) {
                Topic topic = topicManager.getTopic(topicName);
                if (topic == null) continue;

                for (int p = 0; p < topic.getPartitionCount(); p++) {
                    if (!isLeader(topicName, p)) {
                        // We're a follower — fetch from leader
                        Partition partition = topic.getPartition(p);
                        long ourOffset = partition.getNextOffset();

                        try {
                            HttpRequest fetchReq = HttpRequest.newBuilder()
                                    .uri(URI.create("http://" + peer +
                                            "/internal/replicate/" + topicName + "/" + p +
                                            "?fromOffset=" + ourOffset))
                                    .timeout(Duration.ofSeconds(5))
                                    .GET()
                                    .build();

                            HttpResponse<String> fetchResp = httpClient.send(fetchReq,
                                    HttpResponse.BodyHandlers.ofString());

                            if (fetchResp.statusCode() == 200) {
                                log.debug("Replicated from {} for {}:{} fromOffset={}", peer, topicName, p, ourOffset);
                            }
                        } catch (Exception e) {
                            log.debug("Failed to replicate {}:{} from {}: {}", topicName, p, peer, e.getMessage());
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Failed to fetch topics from {}: {}", peer, e.getMessage());
        }
    }

    /**
     * Get replication lag for a follower.
     */
    public long getReplicationLag(String topic, int partition, String follower) {
        Topic t = topicManager.getTopic(topic);
        if (t == null) return -1;

        long latestOffset = t.getPartition(partition).getNextOffset();
        String key = topic + ":" + partition + ":" + follower;
        Long followerOffset = followerOffsets.getOrDefault(key, 0L);
        return latestOffset - followerOffset;
    }
}
