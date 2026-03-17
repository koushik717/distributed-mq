package com.koushik.mq.benchmark;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Benchmark tool for measuring message queue throughput.
 * <p>
 * Usage: java -cp app.jar com.koushik.mq.benchmark.BenchmarkRunner [options]
 * <p>
 * Options:
 *   --host        Broker host (default: localhost:8080)
 *   --topic       Topic name (default: bench-topic)
 *   --producers   Number of producer threads (default: 10)
 *   --messages    Total messages to send (default: 100000)
 *   --size        Message size in bytes (default: 1024)
 */
public class BenchmarkRunner {

    private static final String DEFAULT_HOST = "localhost:8080";
    private static final String DEFAULT_TOPIC = "bench-topic";
    private static final int DEFAULT_PRODUCERS = 10;
    private static final int DEFAULT_MESSAGES = 100_000;
    private static final int DEFAULT_SIZE = 1024;

    public static void main(String[] args) throws Exception {
        String host = getArg(args, "--host", DEFAULT_HOST);
        String topic = getArg(args, "--topic", DEFAULT_TOPIC);
        int producers = Integer.parseInt(getArg(args, "--producers", String.valueOf(DEFAULT_PRODUCERS)));
        int totalMessages = Integer.parseInt(getArg(args, "--messages", String.valueOf(DEFAULT_MESSAGES)));
        int messageSize = Integer.parseInt(getArg(args, "--size", String.valueOf(DEFAULT_SIZE)));

        System.out.println("╔══════════════════════════════════════════╗");
        System.out.println("║     DISTRIBUTED MQ BENCHMARK SUITE      ║");
        System.out.println("╠══════════════════════════════════════════╣");
        System.out.printf("║  Host:       %-27s ║%n", host);
        System.out.printf("║  Topic:      %-27s ║%n", topic);
        System.out.printf("║  Producers:  %-27d ║%n", producers);
        System.out.printf("║  Messages:   %-27s ║%n", String.format("%,d", totalMessages));
        System.out.printf("║  Msg Size:   %-27s ║%n", messageSize + "B");
        System.out.println("╚══════════════════════════════════════════╝");
        System.out.println();

        // Create topic
        createTopic(host, topic);

        // Run producer benchmark
        System.out.println("▸ Starting producer benchmark...");
        long startTime = System.nanoTime();
        AtomicLong successCount = new AtomicLong(0);
        AtomicLong errorCount = new AtomicLong(0);

        String payload = generatePayload(messageSize);
        int messagesPerProducer = totalMessages / producers;

        ExecutorService executor = Executors.newFixedThreadPool(producers);
        CountDownLatch latch = new CountDownLatch(producers);

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        for (int p = 0; p < producers; p++) {
            final int producerId = p;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < messagesPerProducer; i++) {
                        try {
                            String body = String.format(
                                    "{\"key\":\"producer-%d-msg-%d\",\"value\":\"%s\"}",
                                    producerId, i, payload);

                            HttpRequest request = HttpRequest.newBuilder()
                                    .uri(URI.create("http://" + host + "/topics/" + topic + "/messages"))
                                    .header("Content-Type", "application/json")
                                    .POST(HttpRequest.BodyPublishers.ofString(body))
                                    .timeout(Duration.ofSeconds(10))
                                    .build();

                            HttpResponse<String> response = client.send(request,
                                    HttpResponse.BodyHandlers.ofString());

                            if (response.statusCode() == 200) {
                                successCount.incrementAndGet();
                            } else {
                                errorCount.incrementAndGet();
                            }
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                        }

                        // Progress
                        long total = successCount.get() + errorCount.get();
                        if (total % 10000 == 0) {
                            System.out.printf("  Progress: %,d / %,d messages sent%n", total, totalMessages);
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        long elapsedNanos = System.nanoTime() - startTime;
        executor.shutdown();

        double elapsedSeconds = elapsedNanos / 1_000_000_000.0;
        double throughput = successCount.get() / elapsedSeconds;
        double mbPerSec = (successCount.get() * messageSize) / (1024.0 * 1024.0 * elapsedSeconds);

        System.out.println();
        System.out.println("╔══════════════════════════════════════════╗");
        System.out.println("║           BENCHMARK RESULTS              ║");
        System.out.println("╠══════════════════════════════════════════╣");
        System.out.printf("║  Duration:   %-27s ║%n", String.format("%.2f seconds", elapsedSeconds));
        System.out.printf("║  Success:    %-27s ║%n", String.format("%,d messages", successCount.get()));
        System.out.printf("║  Errors:     %-27s ║%n", String.format("%,d messages", errorCount.get()));
        System.out.printf("║  Throughput: %-27s ║%n", String.format("%,.0f msg/sec", throughput));
        System.out.printf("║  Bandwidth:  %-27s ║%n", String.format("%.2f MB/sec", mbPerSec));
        System.out.println("╚══════════════════════════════════════════╝");

        // Run consumer benchmark
        System.out.println();
        System.out.println("▸ Starting consumer benchmark...");
        long consumerStart = System.nanoTime();
        long consumed = 0;
        long offset = 0;

        while (consumed < successCount.get()) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create("http://" + host + "/topics/" + topic +
                                "/messages?offset=" + offset + "&limit=1000&partition=0"))
                        .GET()
                        .timeout(Duration.ofSeconds(10))
                        .build();

                HttpResponse<String> response = client.send(request,
                        HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 200) {
                    // Count messages in response (simple parse)
                    String body = response.body();
                    int count = countOccurrences(body, "\"offset\"");
                    consumed += count;
                    offset += count;
                    if (count == 0) break;
                } else {
                    break;
                }
            } catch (Exception e) {
                break;
            }
        }

        long consumerElapsed = System.nanoTime() - consumerStart;
        double consumerSeconds = consumerElapsed / 1_000_000_000.0;
        double consumerThroughput = consumed / consumerSeconds;

        System.out.printf("  Consumed:    %,d messages in %.2f seconds%n", consumed, consumerSeconds);
        System.out.printf("  Throughput:  %,.0f msg/sec%n", consumerThroughput);
    }

    private static void createTopic(String host, String topic) {
        try {
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + host + "/topics/" + topic + "?partitions=3"))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();
            client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            // Topic may already exist
        }
    }

    private static String generatePayload(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            sb.append((char) ('a' + (i % 26)));
        }
        return sb.toString();
    }

    private static int countOccurrences(String text, String pattern) {
        int count = 0;
        int idx = 0;
        while ((idx = text.indexOf(pattern, idx)) != -1) {
            count++;
            idx += pattern.length();
        }
        return count;
    }

    private static String getArg(String[] args, String name, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) return args[i + 1];
        }
        return defaultValue;
    }
}
