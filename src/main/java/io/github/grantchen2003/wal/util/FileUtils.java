package io.github.grantchen2003.wal.util;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class FileUtils {
    private FileUtils() {
        throw new AssertionError("Cannot instantiate utility class");
    }

    public static void createDirectoryIfNotExist(Path dirPath) throws IOException {
        if (Files.notExists(dirPath)) {
            Files.createDirectories(dirPath);
        }
    }

    public static void createFileIfNotExists(Path filePath) throws IOException {
        if (Files.notExists(filePath)) {
            Files.createFile(filePath);
        }
    }

    public static void appendAndSync(Path filePath, byte[] data) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(filePath.toFile(), true);
             FileChannel channel = fos.getChannel()) {

            ByteBuffer buffer = ByteBuffer.wrap(data);
            while (buffer.hasRemaining()) {
                int written = channel.write(buffer);
                if (written <= 0) {
                    throw new IOException("Failed to write");
                }
            }

            channel.force(true); // fsync
        }
    }
}
