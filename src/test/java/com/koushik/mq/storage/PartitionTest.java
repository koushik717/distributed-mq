package com.koushik.mq.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the append-only Partition log.
 */
class PartitionTest {

    @TempDir
    Path tempDir;

    private Partition partition;

    @BeforeEach
    void setUp() throws IOException {
        partition = new Partition(0, tempDir);
    }

    @Test
    void append_and_read_single_message() throws IOException {
        byte[] key = "user-1".getBytes(StandardCharsets.UTF_8);
        byte[] value = "order-placed".getBytes(StandardCharsets.UTF_8);

        long offset = partition.append(key, value);
        assertEquals(0, offset);

        List<Message> messages = partition.read(0, 10);
        assertEquals(1, messages.size());

        Message msg = messages.get(0);
        assertEquals(0, msg.offset());
        assertArrayEquals(key, msg.key());
        assertArrayEquals(value, msg.value());
        assertTrue(msg.timestamp() > 0);
    }

    @Test
    void offsets_are_monotonically_increasing() throws IOException {
        for (int i = 0; i < 100; i++) {
            long offset = partition.append(
                    ("key-" + i).getBytes(StandardCharsets.UTF_8),
                    ("value-" + i).getBytes(StandardCharsets.UTF_8)
            );
            assertEquals(i, offset);
        }
        assertEquals(100, partition.getNextOffset());
    }

    @Test
    void read_from_middle_offset() throws IOException {
        for (int i = 0; i < 50; i++) {
            partition.append(null, ("msg-" + i).getBytes(StandardCharsets.UTF_8));
        }

        // Read starting from offset 25
        List<Message> messages = partition.read(25, 10);
        assertEquals(10, messages.size());
        assertEquals(25, messages.get(0).offset());
        assertEquals(34, messages.get(9).offset());
    }

    @Test
    void read_with_limit() throws IOException {
        for (int i = 0; i < 100; i++) {
            partition.append(null, ("msg-" + i).getBytes(StandardCharsets.UTF_8));
        }

        List<Message> messages = partition.read(0, 5);
        assertEquals(5, messages.size());
    }

    @Test
    void read_empty_partition() throws IOException {
        List<Message> messages = partition.read(0, 10);
        assertTrue(messages.isEmpty());
    }

    @Test
    void read_beyond_end() throws IOException {
        partition.append(null, "hello".getBytes(StandardCharsets.UTF_8));
        List<Message> messages = partition.read(100, 10);
        assertTrue(messages.isEmpty());
    }

    @Test
    void null_key_is_handled() throws IOException {
        long offset = partition.append(null, "no-key".getBytes(StandardCharsets.UTF_8));
        assertEquals(0, offset);

        List<Message> messages = partition.read(0, 1);
        assertNull(messages.get(0).key());
        assertEquals("no-key", new String(messages.get(0).value(), StandardCharsets.UTF_8));
    }

    @Test
    void large_batch_write_and_read() throws IOException {
        int count = 10_000;
        for (int i = 0; i < count; i++) {
            partition.append(
                    ("k" + i).getBytes(StandardCharsets.UTF_8),
                    ("v" + i).getBytes(StandardCharsets.UTF_8)
            );
        }

        assertEquals(count, partition.getNextOffset());

        // Read all
        List<Message> all = partition.read(0, count);
        assertEquals(count, all.size());
        assertEquals(0, all.get(0).offset());
        assertEquals(count - 1, all.get(count - 1).offset());
    }

    @Test
    void recovery_after_reopen() throws IOException {
        // Write some messages
        partition.append("a".getBytes(StandardCharsets.UTF_8), "1".getBytes(StandardCharsets.UTF_8));
        partition.append("b".getBytes(StandardCharsets.UTF_8), "2".getBytes(StandardCharsets.UTF_8));
        partition.append("c".getBytes(StandardCharsets.UTF_8), "3".getBytes(StandardCharsets.UTF_8));
        partition.close();

        // Reopen — should recover state
        Partition recovered = new Partition(0, tempDir);
        assertEquals(3, recovered.getNextOffset());

        List<Message> messages = recovered.read(0, 10);
        assertEquals(3, messages.size());
        assertEquals("1", new String(messages.get(0).value(), StandardCharsets.UTF_8));
        assertEquals("2", new String(messages.get(1).value(), StandardCharsets.UTF_8));
        assertEquals("3", new String(messages.get(2).value(), StandardCharsets.UTF_8));
        recovered.close();
    }
}
