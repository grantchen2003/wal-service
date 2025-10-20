package io.github.grantchen2003.wal;

import io.github.grantchen2003.wal.util.FileUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;

public class WALManager {
    private static final Path walsDir = Path.of("wals");

    static {
        try {
            FileUtils.createDirectoryIfNotExists(walsDir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String create() throws IOException {
        final String walId = UUID.randomUUID().toString();
        FileUtils.createDirectoryIfNotExists(getWALDirPath(walId));
        FileUtils.createFileIfNotExists(getWALDataPath(walId));
        FileUtils.createFileIfNotExists(getWALIndexPath(walId));
        return walId;
    }

    public void append(String walId, long txId, String payload) throws IOException {
        final WALEntry walEntry = new WALEntry.Builder()
                .withSequenceNum(getLastSequenceNum(walId) + 1)
                .withTxId(txId)
                .withPayload(payload)
                .withTimestamp(Instant.now())
                .build();

        FileUtils.appendAndSync(getWALDataPath(walId), walEntry.toBytes());
    }

    // TODO
    private long getLastSequenceNum(String walId) {
        return 2;
    }

    private Path getWALDirPath(String walId) {
        return walsDir.resolve(walId);
    }

    private Path getWALDataPath(String walId) {
        return getWALDirPath(walId).resolve("data.bin");
    }

    private Path getWALIndexPath(String walId) {
        return getWALDirPath(walId).resolve("index.bin");
    }
}
