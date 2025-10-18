package io.github.grantchen2003.wal;

import io.github.grantchen2003.wal.util.FileUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;

public class WALManager {
    private static final Path walsDir = Path.of("wals");

    public String create() throws IOException {
        FileUtils.createDirectoryIfNotExist(walsDir);
        final String walId = UUID.randomUUID().toString();
        FileUtils.createFileIfNotExists(getWALPath(walId));
        return walId;
    }

    public void append(String walId, String payload) throws IOException {
        final byte[] payloadBytes = payload.getBytes(StandardCharsets.UTF_8);
        FileUtils.appendAndSync(getWALPath(walId), createWALEntry(payloadBytes));
    }

    private byte[] createWALEntry(byte[] payload) {
        final int timestampBytes = Long.BYTES;
        final int payloadLengthBytes = Integer.BYTES;
        final int payloadBytes = payload.length;

        final int walEntrySizeBytes = timestampBytes + payloadLengthBytes + payloadBytes;

        final ByteBuffer walEntry = ByteBuffer.allocate(walEntrySizeBytes);
        walEntry.putLong(Instant.now().toEpochMilli());
        walEntry.putInt(payload.length);
        walEntry.put(payload);

        return walEntry.array();
    }

    private Path getWALPath(String walId) {
        return walsDir.resolve(walId + ".bin");
    }
}
