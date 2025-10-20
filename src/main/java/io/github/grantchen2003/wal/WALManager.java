package io.github.grantchen2003.wal;

import io.github.grantchen2003.wal.util.FileUtils;

import java.io.IOException;
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
        final WALEntry walEntry = new WALEntry.Builder()
                .withPayload(payload)
                .withTimestamp(Instant.now())
                .build();

        FileUtils.appendAndSync(getWALPath(walId), walEntry.toBytes());
    }

    private Path getWALPath(String walId) {
        return walsDir.resolve(walId + ".bin");
    }
}
