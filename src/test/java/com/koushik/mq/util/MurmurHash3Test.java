package com.koushik.mq.util;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for MurmurHash3.
 */
class MurmurHash3Test {

    @Test
    void deterministic_output_for_same_input() {
        byte[] data = "hello-world".getBytes(StandardCharsets.UTF_8);
        int hash1 = MurmurHash3.hash32(data);
        int hash2 = MurmurHash3.hash32(data);
        assertEquals(hash1, hash2);
    }

    @Test
    void different_inputs_produce_different_hashes() {
        int h1 = MurmurHash3.hash32("foo".getBytes(StandardCharsets.UTF_8));
        int h2 = MurmurHash3.hash32("bar".getBytes(StandardCharsets.UTF_8));
        assertNotEquals(h1, h2);
    }

    @Test
    void reasonable_distribution_across_buckets() {
        int buckets = 8;
        Map<Integer, Integer> distribution = new HashMap<>();

        for (int i = 0; i < 10000; i++) {
            byte[] data = ("key-" + i).getBytes(StandardCharsets.UTF_8);
            int bucket = Math.abs(MurmurHash3.hash32(data)) % buckets;
            distribution.merge(bucket, 1, Integer::sum);
        }

        // Each bucket should have roughly 10000/8 = 1250 keys
        // Allow 30% deviation
        for (int i = 0; i < buckets; i++) {
            int count = distribution.getOrDefault(i, 0);
            assertTrue(count > 800, "Bucket " + i + " has too few keys: " + count);
            assertTrue(count < 1700, "Bucket " + i + " has too many keys: " + count);
        }
    }

    @Test
    void empty_input() {
        int hash = MurmurHash3.hash32(new byte[0]);
        // Should not throw, just produce a deterministic value
        assertEquals(hash, MurmurHash3.hash32(new byte[0]));
    }
}
