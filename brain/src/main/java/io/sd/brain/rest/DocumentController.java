package io.sd.brain.rest;

import ai.djl.translate.TranslateException;
import io.sd.brain.ipfs.IpfsClient;
import io.sd.brain.emb.EmbeddingService;
import io.sd.brain.index.VersionVectorService;
import io.sd.brain.index.VersionVectorService.IndexUpdate;
import io.sd.brain.consensus.AckService;
import io.sd.brain.consensus.AckCollector;
import io.sd.brain.pubsub.ClusterState;
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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;

@RestController
public class DocumentController {
    private final ObjectMapper om;
    private final IpfsClient ipfs;
    private final EmbeddingService embeddingService;
    private final VersionVectorService versionVectorService;
    private final PubSubService pubSubService;
    private final String pubSubTopic;
    private final AckService ackService;
    private final int quorum;


    public DocumentController(ObjectMapper om, @Value("${ipfs.api}") String apiUrl,
                              EmbeddingService embeddingService,
                              VersionVectorService versionVectorService,
                              PubSubService pubSubService,
                              AckService ackService,
                              @Value("${pubsub.topic:sd-index}") String pubSubTopic,
                              @Value("${cluster.quorum:1}") int quorum) {
        this.om = om;
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

        List<String> cids = (upd.cids() != null)
                ? upd.cids()
                : versionVectorService.currentCids(cid);

        // Publicar o manifest como ficheiro no IPFS
        byte[] manifestJson = om.writeValueAsBytes(Map.of(
                "version", upd.version(),
                "prev_version", upd.prev_version(),
                "cids", cids
        ));
        String manifestCid = ipfs.add("manifest-v" + upd.version() + ".json", manifestJson, true);

        var prepare = Map.of(
                "kind", "prepare",
                "version", upd.version(),
                "prev_version", upd.prev_version(),
                "cid", upd.cid(),
                "name", name,
                "vector_dim", upd.vector_dim(),
                "vector", upd.vector(),
                "cids_hash", upd.cids_hash(),
                "manifest_cid", manifestCid

        );
        pubSubService.publishJson(pubSubTopic, prepare);

        int observed = 0;
        try { observed = pubSubService.peersCount(pubSubTopic); } catch (Exception ignore) {}
        int majority = Math.max(quorum, (observed == 0 ? quorum : (observed / 2 + 1)));

        AckCollector col = ackService.startWait(upd.version(), majority);

        boolean ok = col.awaitMajoritySameHash(Duration.ofSeconds(5));
        if (!ok) throw new RuntimeException("Sem maioria consistente (ACK)");

        // 3) COMMIT
        var commit = Map.of("kind", "commit", "version", upd.version(), "hash", col.getAgreedHash());
        pubSubService.publishJson(pubSubTopic, commit);

        // 4) promove no líder
        versionVectorService.commit(upd);

        // 5) Atualiza snapshot do índice e anuncia no heartbeat
        var line = Map.of(
                "kind", "index_update",
                "cid", upd.cid(),
                "name", upd.name(),
                "version", upd.version(),
                "vector_dim", upd.vector_dim(),
                "vector", upd.vector(),
                "ts", upd.ts()
        );

        java.nio.file.Path leaderIdx = java.nio.file.Paths.get("data/leader-index.jsonl");
        java.nio.file.Files.createDirectories(leaderIdx.getParent());
        java.nio.file.Files.writeString(
                leaderIdx,
                new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(line) + System.lineSeparator(),
                java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND
        );

        String indexCid = ipfs.add("index.jsonl", java.nio.file.Files.readAllBytes(leaderIdx), true);
        ClusterState.setVersionAndIndexCid(upd.version(), indexCid);

        ackService.clear(upd.version());

        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "cid", cid, "name", name, "version", upd.version(), "vector_dim", upd.vector_dim()
        ));
    }
}