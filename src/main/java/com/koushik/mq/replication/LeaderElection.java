package com.koushik.mq.replication;

import com.koushik.mq.config.MqConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;

/**
 * Leader election when leader fails.
 * <p>
 * New leader elected from ISR (In-Sync Replicas) only — prevents data loss.
 * Uses heartbeat-based failure detection.
 */
@Slf4j
@Component
public class LeaderElection {

    private final MqConfig config;
    private final BrokerRegistry brokerRegistry;
    private final ReplicationManager replicationManager;
    private final ScheduledExecutorService electionExecutor;

    private static final long HEARTBEAT_TIMEOUT_MS = 10_000; // 10 seconds

    public LeaderElection(MqConfig config, BrokerRegistry brokerRegistry,
                          ReplicationManager replicationManager) {
        this.config = config;
        this.brokerRegistry = brokerRegistry;
        this.replicationManager = replicationManager;

        this.electionExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "leader-election");
            t.setDaemon(true);
            return t;
        });

        if (brokerRegistry.hasPeers()) {
            electionExecutor.scheduleAtFixedRate(this::checkLeaderHealth, 10, 5, TimeUnit.SECONDS);
            log.info("LeaderElection started — heartbeat timeout: {}ms", HEARTBEAT_TIMEOUT_MS);
        }
    }

    /**
     * Check if current leaders are still alive. If not, elect new leader from ISR.
     */
    private void checkLeaderHealth() {
        try {
            for (Map.Entry<String, String> entry : replicationManager.getPartitionLeaders().entrySet()) {
                String partKey = entry.getKey(); // "topic:partition"
                String leader = entry.getValue();

                if (!"self".equals(leader) && !brokerRegistry.isPeerAlive(leader)) {
                    log.warn("Leader {} for {} is DOWN — initiating election", leader, partKey);
                    electNewLeader(partKey);
                }
            }
        } catch (Exception e) {
            log.error("Leader health check error: {}", e.getMessage());
        }
    }

    /**
     * Elect a new leader from ISR for the given partition.
     */
    private void electNewLeader(String partKey) {
        Set<String> isr = replicationManager.getInSyncReplicas().get(partKey);

        if (isr == null || isr.isEmpty()) {
            log.error("No ISR available for {} — cannot elect leader", partKey);
            return;
        }

        // Find first alive ISR member
        for (String candidate : isr) {
            if ("self".equals(candidate) || brokerRegistry.isPeerAlive(candidate)) {
                log.info("Elected new leader for {}: {}", partKey, candidate);
                replicationManager.getPartitionLeaders().put(partKey, candidate);

                // If we're the new leader
                if ("self".equals(candidate)) {
                    String[] parts = partKey.split(":");
                    String topic = parts[0];
                    int partition = Integer.parseInt(parts[1]);
                    replicationManager.assignLeader(topic, partition);
                }
                return;
            }
        }

        log.error("All ISR members for {} are down — cannot elect leader", partKey);
    }

    /**
     * Check if this broker should become leader for a partition (used during startup).
     * Simple strategy: lowest broker ID among alive peers becomes leader.
     */
    public boolean shouldBeLeader(String topic, int partition) {
        if (!brokerRegistry.hasPeers()) return true; // single broker

        int myId = config.getBrokerId();
        // Use partition assignment: partition % total_brokers == myBrokerId
        int totalBrokers = brokerRegistry.getBrokers().size() + 1;
        return partition % totalBrokers == (myId - 1);
    }
}
