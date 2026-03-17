package com.koushik.mq.replication;

import com.koushik.mq.config.MqConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;

/**
 * Tracks known brokers in the cluster and their health via heartbeats.
 */
@Slf4j
@Component
@Getter
public class BrokerRegistry {

    private final int localBrokerId;
    private final ConcurrentMap<String, BrokerInfo> brokers = new ConcurrentHashMap<>();
    private final ScheduledExecutorService heartbeatExecutor;

    public BrokerRegistry(MqConfig config) {
        this.localBrokerId = config.getBrokerId();

        // Parse peers: "broker2:9093,broker3:9094"
        String peers = config.getPeers();
        if (peers != null && !peers.isBlank()) {
            for (String peer : peers.split(",")) {
                String trimmed = peer.trim();
                if (!trimmed.isEmpty()) {
                    brokers.put(trimmed, new BrokerInfo(trimmed, System.currentTimeMillis()));
                    log.info("Registered peer: {}", trimmed);
                }
            }
        }

        // Start heartbeat loop
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "broker-heartbeat");
            t.setDaemon(true);
            return t;
        });

        if (!brokers.isEmpty()) {
            heartbeatExecutor.scheduleAtFixedRate(this::sendHeartbeats, 5, 5, TimeUnit.SECONDS);
        }

        log.info("BrokerRegistry initialized — brokerId={}, peers={}", localBrokerId, brokers.keySet());
    }

    private void sendHeartbeats() {
        for (Map.Entry<String, BrokerInfo> entry : brokers.entrySet()) {
            String peer = entry.getKey();
            try {
                // Attempt HTTP health check
                java.net.http.HttpClient client = java.net.http.HttpClient.newBuilder()
                        .connectTimeout(java.time.Duration.ofSeconds(2))
                        .build();
                java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                        .uri(java.net.URI.create("http://" + peer + "/actuator/health"))
                        .timeout(java.time.Duration.ofSeconds(2))
                        .GET()
                        .build();
                java.net.http.HttpResponse<String> response = client.send(request,
                        java.net.http.HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 200) {
                    entry.getValue().setLastHeartbeat(System.currentTimeMillis());
                    entry.getValue().setAlive(true);
                } else {
                    entry.getValue().setAlive(false);
                }
            } catch (Exception e) {
                entry.getValue().setAlive(false);
                log.debug("Heartbeat failed for peer {}: {}", peer, e.getMessage());
            }
        }
    }

    public boolean isPeerAlive(String peer) {
        BrokerInfo info = brokers.get(peer);
        return info != null && info.isAlive();
    }

    public List<String> getAlivePeers() {
        List<String> alive = new ArrayList<>();
        for (Map.Entry<String, BrokerInfo> entry : brokers.entrySet()) {
            if (entry.getValue().isAlive()) {
                alive.add(entry.getKey());
            }
        }
        return alive;
    }

    public boolean hasPeers() {
        return !brokers.isEmpty();
    }

    @Getter
    public static class BrokerInfo {
        private final String address;
        private volatile long lastHeartbeat;
        private volatile boolean alive;

        public BrokerInfo(String address, long lastHeartbeat) {
            this.address = address;
            this.lastHeartbeat = lastHeartbeat;
            this.alive = false; // starts as unknown
        }

        public void setLastHeartbeat(long ts) { this.lastHeartbeat = ts; }
        public void setAlive(boolean alive) { this.alive = alive; }
    }
}
