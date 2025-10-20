package io.github.grantchen2003.wal;

import io.github.grantchen2003.wal.util.ByteUtils;
import io.github.grantchen2003.wal.util.ChecksumUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;

public class WALEntry {
    private final int sizeInBytes;
    private final byte[] bytes;

    private WALEntry(Builder builder) {
        final byte[] epochMilliTimestamp = ByteUtils.longToBytes(builder.timestamp.toEpochMilli());
        final byte[] payload = builder.payload.getBytes(StandardCharsets.UTF_8);
        final byte[] payloadLength = ByteUtils.intToBytes(payload.length);
        final byte[] checksum = ByteUtils.intToBytes(ChecksumUtils.computeCRC32(epochMilliTimestamp, payloadLength, payload));

        final byte[][] body = {epochMilliTimestamp, payloadLength, payload, checksum};
        sizeInBytes = Arrays.stream(body).mapToInt(byteArr-> byteArr.length).sum();

        final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + sizeInBytes);
        buffer.putInt(sizeInBytes);
        for (byte[] part : body) {
            buffer.put(part);
        }
        bytes = buffer.array();
    }

    public byte[] toBytes() {
        return bytes;
    }

    public int sizeInBytes() {
        return sizeInBytes;
    }

    public static class Builder {
        private String payload;
        private Instant timestamp;

        public Builder withPayload(String payload) {
            this.payload = payload;
            return this;
        }

        public Builder withTimestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public WALEntry build() {
            if (payload == null) {
                throw new IllegalStateException("No payload");
            }

            if (timestamp == null) {
                throw new IllegalStateException("No timestamp");
            }

            return new WALEntry(this);
        }
    }
}
