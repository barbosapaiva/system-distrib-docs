package io.sd.brain.rest;

import ai.djl.translate.TranslateException;
import io.sd.brain.ipfs.IpfsClient;
import io.sd.brain.emb.EmbeddingService;
import io.sd.brain.index.VersionVectorService;
import io.sd.brain.index.VersionVectorService.IndexUpdate; // se usares a classe aninhada
import io.sd.brain.pubsub.PubSubService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@RestController
public class DocumentController {

    private final IpfsClient ipfs;
    private final EmbeddingService embeddingService;
    private final VersionVectorService versionVectorService;
    private final PubSubService pubSubService;
    private final String pubSubTopic;

    public DocumentController(@Value("${ipfs.api}") String apiUrl,
                              EmbeddingService embeddingService,
                              VersionVectorService versionVectorService,
                              PubSubService pubSubService,
                              @Value("${pubsub.topic:sd-index}") String pubSubTopic) {
        this.ipfs = new IpfsClient(apiUrl);   // igual ao S1
        this.embeddingService = embeddingService;
        this.versionVectorService = versionVectorService;
        this.pubSubService = pubSubService;
        this.pubSubTopic = pubSubTopic;
    }

    @PostMapping(value = "/documents", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Map<String, Object>> upload(@RequestPart("file") MultipartFile file)
            throws IOException, TranslateException {

        final byte[] content = file.getBytes();
        final String name = file.getOriginalFilename();

        String cid = ipfs.add(name, content, true);

        String text = new String(content, StandardCharsets.UTF_8);
        float[] vector = embeddingService.embed(text);

        IndexUpdate upd = versionVectorService.buildCandidate(
                cid, name, vector.length, vector
        );

        var msg = Map.of(
                "kind", "index_update",
                "cid", cid,
                "name", name,
                "version", upd.version(),
                "vector_dim", upd.vector_dim(),
                "vector", vector,
                "ts", System.currentTimeMillis()
        );
        pubSubService.publishJson(pubSubTopic, msg);

        versionVectorService.commit(upd);

        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "cid", cid, "name", name, "version", upd.version(), "vector_dim", upd.vector_dim()
        ));
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of("leader", true);
    }
}