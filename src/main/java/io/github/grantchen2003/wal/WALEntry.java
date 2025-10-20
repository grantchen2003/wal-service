package io.github.grantchen2003.wal;

import io.github.grantchen2003.wal.util.ByteUtils;
import io.github.grantchen2003.wal.util.ChecksumUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;

public class WALEntry {
    private final byte[] bytes;

    private WALEntry(Builder builder) {
        final byte[] sequenceNum = ByteUtils.longToBytes(builder.sequenceNum);
        final byte[] txId = ByteUtils.longToBytes(builder.txId);
        final byte[] epochMilliTimestamp = ByteUtils.longToBytes(builder.timestamp.toEpochMilli());
        final byte[] payload = builder.payload.getBytes(StandardCharsets.UTF_8);
        final byte[] payloadLength = ByteUtils.intToBytes(payload.length);
        final byte[] checksum = ByteUtils.intToBytes(ChecksumUtils.computeCRC32(epochMilliTimestamp, payloadLength, payload));

        final byte[][] body = {sequenceNum, txId, epochMilliTimestamp, payloadLength, payload, checksum};
        final int bodySizeInBytes = Arrays.stream(body).mapToInt(byteArr -> byteArr.length).sum();

        final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + bodySizeInBytes);
        buffer.putInt(bodySizeInBytes);
        for (byte[] part : body) {
            buffer.put(part);
        }
        bytes = buffer.array();
    }

    public byte[] toBytes() {
        return bytes;
    }

    public static class Builder {
        private String payload;
        private long sequenceNum;
        private Instant timestamp;
        private long txId;

        public Builder withPayload(String payload) {
            this.payload = payload;
            return this;
        }

        public Builder withSequenceNum(long sequenceNum) {
            this.sequenceNum = sequenceNum;
            return this;
        }

        public Builder withTimestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder withTxId(long txId) {
            this.txId = txId;
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
