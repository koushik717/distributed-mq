package com.koushik.mq.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Append-only partition log backed by a binary file on disk.
 * <p>
 * Each record is stored in a compact binary format:
 * [8B offset][8B timestamp][4B keyLen][key bytes][4B valueLen][value bytes]
 * <p>
 * Writes are serialized (one writer at a time) via a ReentrantLock.
 * Reads are lock-free — each read opens its own FileChannel position.
 * This mirrors how Kafka's log segments work.
 */
public class Partition {

    private final int partitionId;
    private final Path logFile;
    private final FileChannel writeChannel;
    private final ReentrantLock writeLock = new ReentrantLock();

    private volatile long nextOffset = 0;

    // In-memory index: offset → file position for O(1) seek
    private final List<Long> offsetIndex = new ArrayList<>();

    public Partition(int partitionId, Path dataDir) throws IOException {
        this.partitionId = partitionId;

        Path partitionDir = dataDir.resolve("partition-" + partitionId);
        Files.createDirectories(partitionDir);
        this.logFile = partitionDir.resolve("log.bin");

        this.writeChannel = FileChannel.open(logFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);

        // Recover state from existing log file
        recover();
    }

    /**
     * Append a message to this partition's log.
     *
     * @return the offset assigned to this message
     */
    public long append(byte[] key, byte[] value) throws IOException {
        writeLock.lock();
        try {
            long offset = nextOffset;
            long timestamp = System.currentTimeMillis();

            int keyLen = (key != null) ? key.length : 0;
            int valLen = (value != null) ? value.length : 0;
            int totalSize = 8 + 8 + 4 + keyLen + 4 + valLen;

            ByteBuffer buffer = ByteBuffer.allocate(totalSize);
            buffer.putLong(offset);
            buffer.putLong(timestamp);
            buffer.putInt(keyLen);
            if (keyLen > 0) buffer.put(key);
            buffer.putInt(valLen);
            if (valLen > 0) buffer.put(value);
            buffer.flip();

            // Record file position before writing
            long filePosition = writeChannel.size();
            synchronized (offsetIndex) {
                offsetIndex.add(filePosition);
            }

            // Append to end of file
            writeChannel.position(filePosition);
            while (buffer.hasRemaining()) {
                writeChannel.write(buffer);
            }
            writeChannel.force(false); // fsync data (not metadata) for durability

            nextOffset = offset + 1;
            return offset;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Read messages starting from the given offset.
     *
     * @param fromOffset  starting offset (inclusive)
     * @param maxMessages maximum number of messages to return
     * @return list of messages read
     */
    public List<Message> read(long fromOffset, int maxMessages) throws IOException {
        List<Message> messages = new ArrayList<>();

        if (fromOffset >= nextOffset || maxMessages <= 0) {
            return messages;
        }

        // Use the offset index to seek directly to the right file position
        long filePosition;
        synchronized (offsetIndex) {
            if (fromOffset >= offsetIndex.size()) {
                return messages;
            }
            filePosition = offsetIndex.get((int) fromOffset);
        }

        // Open a separate read channel so reads don't interfere with writes
        try (FileChannel readChannel = FileChannel.open(logFile, StandardOpenOption.READ)) {
            readChannel.position(filePosition);

            int count = 0;
            while (count < maxMessages) {
                Message msg = readOneMessage(readChannel);
                if (msg == null) break; // EOF
                messages.add(msg);
                count++;
            }
        }

        return messages;
    }

    /**
     * Read a single message from the channel at its current position.
     * Returns null on EOF.
     */
    private Message readOneMessage(FileChannel channel) throws IOException {
        // Read header: offset (8) + timestamp (8) + keyLen (4) = 20 bytes
        ByteBuffer header = ByteBuffer.allocate(20);
        int bytesRead = fillBuffer(channel, header);
        if (bytesRead < 20) return null;
        header.flip();

        long offset = header.getLong();
        long timestamp = header.getLong();
        int keyLen = header.getInt();

        // Read key
        byte[] key = null;
        if (keyLen > 0) {
            ByteBuffer keyBuf = ByteBuffer.allocate(keyLen);
            if (fillBuffer(channel, keyBuf) < keyLen) return null;
            keyBuf.flip();
            key = new byte[keyLen];
            keyBuf.get(key);
        }

        // Read value length
        ByteBuffer valLenBuf = ByteBuffer.allocate(4);
        if (fillBuffer(channel, valLenBuf) < 4) return null;
        valLenBuf.flip();
        int valLen = valLenBuf.getInt();

        // Read value
        byte[] value = null;
        if (valLen > 0) {
            ByteBuffer valBuf = ByteBuffer.allocate(valLen);
            if (fillBuffer(channel, valBuf) < valLen) return null;
            valBuf.flip();
            value = new byte[valLen];
            valBuf.get(value);
        }

        return new Message(offset, timestamp, key, value);
    }

    /**
     * Fill a buffer completely from the channel. Returns total bytes read.
     */
    private int fillBuffer(FileChannel channel, ByteBuffer buffer) throws IOException {
        int totalRead = 0;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read == -1) break;
            totalRead += read;
        }
        return totalRead;
    }

    /**
     * Recover state by scanning the existing log file to rebuild the offset index.
     */
    private void recover() throws IOException {
        long fileSize = writeChannel.size();
        if (fileSize == 0) return;

        writeChannel.position(0);
        while (writeChannel.position() < fileSize) {
            long pos = writeChannel.position();

            ByteBuffer header = ByteBuffer.allocate(20);
            int read = fillBuffer(writeChannel, header);
            if (read < 20) break;
            header.flip();

            long offset = header.getLong();
            header.getLong(); // skip timestamp
            int keyLen = header.getInt();

            // Skip key
            if (keyLen > 0) {
                writeChannel.position(writeChannel.position() + keyLen);
            }

            // Read value length and skip value
            ByteBuffer valLenBuf = ByteBuffer.allocate(4);
            read = fillBuffer(writeChannel, valLenBuf);
            if (read < 4) break;
            valLenBuf.flip();
            int valLen = valLenBuf.getInt();

            if (valLen > 0) {
                writeChannel.position(writeChannel.position() + valLen);
            }

            synchronized (offsetIndex) {
                offsetIndex.add(pos);
            }
            nextOffset = offset + 1;
        }

        // Position write channel at end for future appends
        writeChannel.position(fileSize);
    }

    public int getPartitionId() {
        return partitionId;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public long getLatestOffset() {
        return nextOffset > 0 ? nextOffset - 1 : -1;
    }

    /**
     * Close the write channel. Called on shutdown.
     */
    public void close() throws IOException {
        writeChannel.close();
    }
}
