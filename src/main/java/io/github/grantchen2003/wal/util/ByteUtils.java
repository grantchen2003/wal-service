package io.github.grantchen2003.wal.util;

import java.nio.ByteBuffer;

public class ByteUtils {
    private ByteUtils() {}

    public static byte[] longToBytes(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }

    public static byte[] intToBytes(int value) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
    }

    public static byte[] concat(byte[]... arrays) {
        int totalLength = 0;
        for (byte[] arr : arrays) {
            totalLength += arr.length;
        }

        final ByteBuffer buffer = ByteBuffer.allocate(totalLength);
        for (byte[] arr : arrays) {
            buffer.put(arr);
        }
        return buffer.array();
    }
}
