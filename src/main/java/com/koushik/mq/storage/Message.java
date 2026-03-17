package com.koushik.mq.storage;

/**
 * A single message in the append-only log.
 * <p>
 * Binary format on disk:
 * [8B offset][8B timestamp][4B keyLen][key bytes][4B valueLen][value bytes]
 */
public record Message(
        long offset,
        long timestamp,
        byte[] key,
        byte[] value
) {

    /**
     * Returns the total byte size of this message when serialized to disk.
     */
    public int serializedSize() {
        int keyLen = (key != null) ? key.length : 0;
        int valLen = (value != null) ? value.length : 0;
        return 8 + 8 + 4 + keyLen + 4 + valLen; // offset + timestamp + keyLen + key + valLen + value
    }
}
