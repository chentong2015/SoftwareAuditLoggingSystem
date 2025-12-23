package com.audit.client.util;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class FileHelper {

    public static void applyOnFiles(String actionDescription, File queueFile, String pattern, Consumer<Path> action) {
        try {
            Path parentDir = Paths.get(queueFile.getParent()).toAbsolutePath().normalize();
            try (Stream<Path> pathStream = Files.list(parentDir)) {
                pathStream.filter(path -> path.getFileName().toString().startsWith(pattern))
                        .forEach(action::accept);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
