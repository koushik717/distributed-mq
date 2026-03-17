package com.koushik.mq.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Prometheus metrics for the message queue.
 */
@Component
public class MqMetrics {

    private final MeterRegistry registry;

    // Cache counters by topic+partition to avoid re-creation
    private final ConcurrentMap<String, Counter> produceCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> consumeCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> bytesWrittenCounters = new ConcurrentHashMap<>();

    public MqMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void recordProduce(String topic, int partition) {
        String key = topic + ":" + partition;
        produceCounters.computeIfAbsent(key, k ->
                Counter.builder("mq_messages_produced_total")
                        .tag("topic", topic)
                        .tag("partition", String.valueOf(partition))
                        .description("Total messages produced")
                        .register(registry)
        ).increment();
    }

    public void recordConsume(String topic, int partition, int count) {
        String key = topic + ":" + partition;
        consumeCounters.computeIfAbsent(key, k ->
                Counter.builder("mq_messages_consumed_total")
                        .tag("topic", topic)
                        .tag("partition", String.valueOf(partition))
                        .description("Total messages consumed")
                        .register(registry)
        ).increment(count);
    }

    public void recordBytesWritten(String topic, int bytes) {
        bytesWrittenCounters.computeIfAbsent(topic, k ->
                Counter.builder("mq_bytes_written_total")
                        .tag("topic", topic)
                        .description("Total bytes written")
                        .register(registry)
        ).increment(bytes);
    }
}
