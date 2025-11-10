package io.sd.brain.rest;

import ai.djl.translate.TranslateException;
import io.sd.brain.ipfs.IpfsClient;
import io.sd.brain.emb.EmbeddingService;
import io.sd.brain.index.VersionVectorService;
import io.sd.brain.index.VersionVectorService.IndexUpdate;
import io.sd.brain.consensus.AckService;
import io.sd.brain.consensus.AckCollector;
import io.sd.brain.pubsub.PubSubService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

@RestController
public class DocumentController {

    private final IpfsClient ipfs;
    private final EmbeddingService embeddingService;
    private final VersionVectorService versionVectorService;
    private final PubSubService pubSubService;
    private final String pubSubTopic;
    private final AckService ackService;
    private final int quorum;

    public DocumentController(@Value("${ipfs.api}") String apiUrl,
                              EmbeddingService embeddingService,
                              VersionVectorService versionVectorService,
                              PubSubService pubSubService,
                              AckService ackService,
                              @Value("${pubsub.topic:sd-index}") String pubSubTopic,
                              @Value("${cluster.quorum:1}") int quorum) {
        this.ipfs = new IpfsClient(apiUrl);
        this.embeddingService = embeddingService;
        this.versionVectorService = versionVectorService;
        this.pubSubService = pubSubService;
        this.ackService = ackService;
        this.pubSubTopic = pubSubTopic;
        this.quorum = quorum;
    }

    @PostMapping(value = "/documents", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Map<String, Object>> upload(@RequestPart("file") MultipartFile file)
            throws IOException, TranslateException {

        final byte[] content = file.getBytes();
        final String name = file.getOriginalFilename();

        String cid = ipfs.add(name, content, true);

        String text = new String(content, StandardCharsets.UTF_8);
        float[] vector = embeddingService.embed(text);

        IndexUpdate upd = versionVectorService.buildCandidate(cid, name, vector.length, vector);

        var prepare = Map.of(
                "kind", "prepare",
                "version", upd.version(),
                "prev_version", upd.prev_version(),   // se tiveres
                "cid", upd.cid(),
                "name", name,
                "vector_dim", upd.vector_dim(),
                "vector", upd.vector(),
                "cids_hash", upd.cids_hash()          // <-- IMPORTANTE
        );
        pubSubService.publishJson(pubSubTopic, prepare);

        AckCollector col = ackService.startWait(upd.version(), quorum);

        boolean ok = col.awaitMajoritySameHash(Duration.ofSeconds(15));
        if (!ok) throw new RuntimeException("Sem maioria consistente (ACK)");

        // 3) COMMIT
        var commit = Map.of("kind", "commit", "version", upd.version(), "hash", col.getAgreedHash());
        pubSubService.publishJson(pubSubTopic, commit);

        // 4) promove no líder
        versionVectorService.commit(upd);

        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "cid", cid, "name", name, "version", upd.version(), "vector_dim", upd.vector_dim()
        ));
    }
}