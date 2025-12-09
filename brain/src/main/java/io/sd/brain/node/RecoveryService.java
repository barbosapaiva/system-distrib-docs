package io.sd.brain.node;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.sd.brain.search.InMemoryIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@Service
public class RecoveryService {

    private static final Logger log = LoggerFactory.getLogger(RecoveryService.class);

    public record RecoveryState(List<String> cids, long version) { }

    private final ObjectMapper mapper;
    private final InMemoryIndex inMemoryIndex;

    public RecoveryService(ObjectMapper mapper, InMemoryIndex inMemoryIndex) {
        this.mapper = mapper;
        this.inMemoryIndex = inMemoryIndex;
    }

    public RecoveryState recover(Path outFile) {
        if (!Files.exists(outFile)) {
            log.info("Sem ficheiro de índice em {} - nada para recuperar.", outFile);
            return new RecoveryState(List.of(), 0L);
        }

        List<String> cids = new ArrayList<>();
        long lastVersion = 0L;

        try {
            for (String line : Files.readAllLines(outFile)) {
                if (line == null || line.isBlank()) continue;

                JsonNode node = mapper.readTree(line);
                if (!"index_update".equals(node.path("kind").asText())) continue;

                String cid = node.path("cid").asText(null);
                if (cid == null || cid.isBlank()) continue;
                cids.add(cid);

                long v = node.path("version").asLong(0L);
                if (v > lastVersion) lastVersion = v;

                if (node.has("vector") && node.get("vector").isArray()) {
                    ArrayNode arr = (ArrayNode) node.get("vector");
                    float[] vec = new float[arr.size()];
                    for (int i = 0; i < arr.size(); i++) {
                        vec[i] = (float) arr.get(i).asDouble();
                    }
                    inMemoryIndex.addOrUpdate(cid, vec);
                }
            }

            log.info("Recovery concluído: {} documentos, versão {}.", cids.size(), lastVersion);
        } catch (IOException e) {
            log.warn("Erro ao recuperar a partir de {}: {}", outFile, e.toString());
        }

        return new RecoveryState(cids, lastVersion);
    }
}