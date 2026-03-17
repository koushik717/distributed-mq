package com.koushik.mq.util;

/**
 * MurmurHash3 32-bit implementation.
 * Same hash algorithm Kafka uses internally for key-based partitioning.
 */
public final class MurmurHash3 {

    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;
    private static final int SEED = 104729; // arbitrary prime seed

    private MurmurHash3() {
    }

    /**
     * Compute 32-bit MurmurHash3 for the given byte array.
     */
    public static int hash32(byte[] data) {
        return hash32(data, 0, data.length, SEED);
    }

    public static int hash32(byte[] data, int offset, int length, int seed) {
        int h1 = seed;
        int nblocks = length >> 2; // length / 4

        // body — process 4-byte blocks
        for (int i = 0; i < nblocks; i++) {
            int i4 = offset + (i << 2);
            int k1 = (data[i4] & 0xff)
                    | ((data[i4 + 1] & 0xff) << 8)
                    | ((data[i4 + 2] & 0xff) << 16)
                    | ((data[i4 + 3] & 0xff) << 24);

            k1 *= C1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= C2;

            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // tail — handle remaining bytes
        int tail = offset + (nblocks << 2);
        int k1 = 0;
        switch (length & 3) {
            case 3:
                k1 ^= (data[tail + 2] & 0xff) << 16;
            case 2:
                k1 ^= (data[tail + 1] & 0xff) << 8;
            case 1:
                k1 ^= (data[tail] & 0xff);
                k1 *= C1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= C2;
                h1 ^= k1;
        }

        // finalization — avalanche
        h1 ^= length;
        h1 ^= (h1 >>> 16);
        h1 *= 0x85ebca6b;
        h1 ^= (h1 >>> 13);
        h1 *= 0xc2b2ae35;
        h1 ^= (h1 >>> 16);

        return h1;
    }
}
