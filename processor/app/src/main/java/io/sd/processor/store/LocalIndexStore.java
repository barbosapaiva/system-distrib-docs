package io.sd.processor.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.sd.processor.model.IndexUpdate;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

public class LocalIndexStore {

    private final Path path;
    private final ObjectMapper mapper = new ObjectMapper();

    public LocalIndexStore(String file) throws IOException {
        this.path = Path.of(file);
        Files.createDirectories(path.getParent() == null ? Path.of(".") : path.getParent());
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
    }

    public synchronized void append(IndexUpdate upd) throws IOException {
        String line = mapper.writeValueAsString(upd) + "\n";
        Files.writeString(path, line, StandardCharsets.UTF_8, StandardOpenOption.APPEND);
    }
}