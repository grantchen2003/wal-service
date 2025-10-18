package io.github.grantchen2003.wal.util;

import java.io.IOException;
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

    public static void appendBytesToFile(Path filePath, byte[] data) throws IOException {
        Files.write(filePath, data, StandardOpenOption.APPEND);
    }
}
