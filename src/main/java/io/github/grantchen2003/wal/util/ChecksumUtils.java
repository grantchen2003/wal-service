package io.github.grantchen2003.wal.util;

import java.util.zip.CRC32;

public class ChecksumUtils {
    private ChecksumUtils() {}

    public static int computeCRC32(byte[]... arrays) {
        final CRC32 crc = new CRC32();
        for (byte[] arr : arrays) {
            crc.update(arr, 0, arr.length);
        }
        return (int) crc.getValue();
    }
}
