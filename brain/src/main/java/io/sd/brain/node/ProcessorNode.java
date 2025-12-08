package io.sd.brain.node;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.sd.brain.search.InMemoryIndex;
import io.sd.brain.ipfs.IpfsClient;
import io.sd.brain.pubsub.PubSubClient;
import io.sd.brain.search.SearchMessages;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.BufferedSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class ProcessorNode {

    private static final Logger log = LoggerFactory.getLogger(ProcessorNode.class);

    private final String apiBase;            // ex: http://127.0.0.1:5003/api/v0
    private final String topic;              // ex: sd-index
    private final Path outFile;              // ex: data/index-5003.jsonl

    private final ObjectMapper mapper;
    private final okhttp3.OkHttpClient http;

    private final String peerId;             // PEER_ID único por processo

    private final Map<Long, JsonNode> pendingPrepare = new ConcurrentHashMap<>();
    private final Map<Long, float[]> pendingVectors  = new ConcurrentHashMap<>();

    private final List<String> currentCids = new CopyOnWriteArrayList<>();
    private volatile long currentVersion = 0L;

    // - Controlo de agendamento de eleição com jitter
    private ScheduledExecutorService scheduler;

    private final InMemoryIndex inMemoryIndex;

    private final Map<String, List<SearchMessages.SearchItem>> localSearchResults = new ConcurrentHashMap<>();

    // ------------------------ Construtores --------------------
    public ProcessorNode(String apiBaseUrl, String topic, String outFilePath, RecoveryService recoveryService, InMemoryIndex inMemoryIndex) throws IOException {
        this.apiBase = apiBaseUrl.endsWith("/api/v0") ? apiBaseUrl : apiBaseUrl + "/api/v0";
        this.topic = topic;
        this.outFile = Paths.get(outFilePath);
        this.inMemoryIndex = inMemoryIndex;

        Files.createDirectories(outFile.getParent() == null ? Paths.get(".") : outFile.getParent());
        if (!Files.exists(outFile)) Files.createFile(outFile);

        this.mapper = new ObjectMapper();
        this.http = new okhttp3.OkHttpClient.Builder()
                .callTimeout(Duration.ofMinutes(1))
                .readTimeout(Duration.ofMinutes(1))
                .retryOnConnectionFailure(true)
                .build();

        String host = "peer";
        try { host = InetAddress.getLocalHost().getHostName(); } catch (Exception ignored) {}
        this.peerId = System.getenv().getOrDefault("PEER_ID", host);

        // Estado inicial em memória
        RecoveryService.RecoveryState state = recoveryService.recover(outFile);
        this.currentCids.clear();
        this.currentCids.addAll(state.cids());
        this.currentVersion = state.version();

        log.info("Processor configurado: api={}, topic={}, out={}, docs={}, version={}", apiBase, topic, outFile, currentCids.size(), currentVersion);    }

    public ProcessorNode(InMemoryIndex inMemoryIndex, RecoveryService recoveryService) throws IOException {
        this(
                System.getenv().getOrDefault("IPFS_API", "http://127.0.0.1:5001/api/v0"),
                System.getenv().getOrDefault("PUBSUB_TOPIC", "sd-index"),
                System.getenv().getOrDefault("OUT_FILE", "data/index.jsonl"), recoveryService, inMemoryIndex
        );
    }

    static final class SubMsg {
        public String from;
        public String data;
        public String seqno;
        public List<String> topicIDs;
    }

    private List<String> readLocalCids() {
        try {
            if (!Files.exists(outFile)) return List.of();
            var lines = Files.readAllLines(outFile);
            var out = new ArrayList<String>(lines.size());
            for (String s : lines) {
                if (s == null || s.isBlank()) continue;
                var n = mapper.readTree(s);
                var c = n.path("cid").asText(null);
                if (c != null && !c.isBlank()) out.add(c);
            }
            return out;
        } catch (Exception e) {
            log.warn("Falha a ler CIDs locais: {}", e.toString());
            return List.of();
        }
    }

    private long localVersion() {
        try {
            if (!Files.exists(outFile)) return 0L;
            var all = Files.readAllLines(outFile);
            for (int i = all.size() - 1; i >= 0; i--) {
                var s = all.get(i).trim();
                if (s.isEmpty()) continue;
                var node = mapper.readTree(s);
                return node.path("version").asLong(0L);
            }
        } catch (Exception ignored) {}
        return 0L;
    }

    private static String sha256Hex(String prefix, String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] dig = md.digest(input.getBytes(StandardCharsets.UTF_8));
            var sb = new StringBuilder();
            for (byte b : dig) sb.append(String.format("%02x", b));
            return (prefix == null ? "" : prefix) + sb;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] decodeIpfsData(String b64) {
        if (b64 == null) return new byte[0];
        String s = b64.trim();

        boolean isMbUrl = s.startsWith("u");
        if (isMbUrl) s = s.substring(1);

        int rem = s.length() % 4;
        if (rem == 2) s += "==";
        else if (rem == 3) s += "=";

        try {
            return (isMbUrl ? Base64.getUrlDecoder() : Base64.getDecoder()).decode(s);
        } catch (IllegalArgumentException e) {
            String alt = s.replace('-', '+').replace('_', '/');
            int r = alt.length() % 4;
            if (r == 2) alt += "==";
            else if (r == 3) alt += "=";
            return Base64.getDecoder().decode(alt);
        }
    }

    private static String topicToMultibase(String topicUtf8) {
        return "u" + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(topicUtf8.getBytes(StandardCharsets.UTF_8));
    }

    /** Publica JSON no tópico via /pubsub/pub (multipart 'data'). */
    private void publishJsonToTopic(String topicUtf8, byte[] json) throws IOException {
        String topicMb = topicToMultibase(topicUtf8);
        HttpUrl url = HttpUrl.parse(apiBase + "/pubsub/pub").newBuilder()
                .addQueryParameter("arg", topicMb)
                .build();

        RequestBody dataPart = RequestBody.create(json, MediaType.parse("application/json"));
        MultipartBody body = new MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart("data", "msg.json", dataPart)
                .build();

        Request req = new Request.Builder().url(url).post(body).build();
        try (Response resp = http.newCall(req).execute()) {
            if (!resp.isSuccessful()) {
                String err = resp.body() != null ? resp.body().string() : "";
                throw new IOException("Falha a publicar: HTTP " + resp.code() + " " + err);
            }
        }
    }

    private byte[] ipfsCat(String cid) throws IOException {
        HttpUrl url = HttpUrl.parse(apiBase + "/cat").newBuilder()
                .addQueryParameter("arg", cid).build();
        Request req = new Request.Builder()
                .url(url)
                .post(RequestBody.create(new byte[0], MediaType.parse("application/octet-stream")))
                .build();
        try (Response resp = http.newCall(req).execute()) {
            if (!resp.isSuccessful()) throw new IOException("cat HTTP " + resp.code());
            return resp.body().bytes();
        }
    }

    private void handlePrepare(JsonNode msg) throws IOException {
        long version = msg.path("version").asLong();
        String cid = msg.path("cid").asText(null);
        if (cid == null) { log.warn("prepare sem 'cid'"); return; }

        List<String> cids;
        if (msg.has("cids") && msg.get("cids").isArray()) {
            cids = mapper.convertValue(
                    msg.get("cids"),
                    mapper.getTypeFactory().constructCollectionType(List.class, String.class)
            );
        } else if (msg.has("manifest_cid")) {
            byte[] man = ipfsCat(msg.path("manifest_cid").asText());
            JsonNode manJson = mapper.readTree(man);
            cids = mapper.convertValue(
                    manJson.path("cids"),
                    mapper.getTypeFactory().constructCollectionType(List.class, String.class)
            );
            ((ObjectNode) msg).set("cids", mapper.valueToTree(cids));
        } else {
            cids = new ArrayList<>(currentCids);
            cids.add(msg.path("cid").asText());
            ((ObjectNode) msg).set("cids", mapper.valueToTree(cids));
        }

        String leaderHash = msg.path("cids_hash").asText(null);
        if (leaderHash == null || leaderHash.isBlank()) {
            leaderHash = sha256Hex("sha256:", String.join("|", cids));
        }
        ((ObjectNode) msg).put("cids_hash", leaderHash);

        pendingPrepare.put(version, msg);

        ObjectNode ack = mapper.createObjectNode();
        ack.put("kind", "ack")
                .put("version", version)
                .put("peer_id", peerId)
                .put("status", "ok")
                .put("hash", leaderHash);

        publishJsonToTopic(topic, mapper.writeValueAsBytes(ack));
        log.info("ACK enviado: version={} hash={}", version, leaderHash);
    }

    private void handleCommit(JsonNode msg) {
        long version = msg.path("version").asLong();
        String expectedHash = msg.path("hash").asText(null);

        JsonNode prep = pendingPrepare.remove(version);
        if (prep == null) {
            log.warn("commit v{} sem prepare previamente guardado - ignorado.", version);
            return;
        }

        String cidsHash = prep.path("cids_hash").asText(null);
        if (expectedHash == null || cidsHash == null || !expectedHash.equals(cidsHash)) {
            log.warn("Hash mismatch no commit v{} (esperado={}, local={}) - não persiste.",
                    version, expectedHash, cidsHash);
            return;
        }

        String cid = prep.path("cid").asText();
        String name = prep.path("name").asText("");

        ObjectNode line = mapper.createObjectNode();
        line.put("kind", "index_update");
        line.put("cid", cid);
        line.put("name", prep.path("name").asText(""));
        line.put("version", version);
        int dim = prep.path("vector_dim").asInt(0);
        line.put("vector_dim", dim);
        if (prep.has("vector")) line.set("vector", prep.path("vector"));
        line.put("ts", System.currentTimeMillis());

        try {
            Files.writeString(outFile, mapper.writeValueAsString(line) + System.lineSeparator(),
                    StandardOpenOption.APPEND);
            log.info("Commit aplicado: version={} cid={}", version, cid);
        } catch (IOException e) {
            log.error("Falha a escrever index.jsonl: {}", e.toString());
        }

        float[] vector = null;
        if (prep.has("vector") && prep.get("vector").isArray()) {
            ArrayNode arr = (ArrayNode) prep.get("vector");
            vector = new float[arr.size()];
            for (int i = 0; i < arr.size(); i++) {
                vector[i] = (float) arr.get(i).asDouble();
            }
        }

        // atualiza FAISS em memória
        if (vector != null) {
            inMemoryIndex.addOrUpdate(cid, vector);
            inMemoryIndex.setName(cid, name);
        }

        try {
            List<String> newCids = mapper.convertValue(
                    prep.get("cids"),
                    mapper.getTypeFactory().constructCollectionType(List.class, String.class)
            );
            currentCids.clear();
            currentCids.addAll(newCids);
            currentVersion = version;
        } catch (Exception ignored) {}

        pendingVectors.remove(version);
    }

    private void handleHeartbeat(long leaderVersion, String indexCid) {
        if (indexCid == null || indexCid.isBlank()) return;

        long vLocal = localVersion();
        if (leaderVersion > vLocal) {
            try {
                byte[] bytes = ipfsCat(indexCid);
                Path tmp = outFile.resolveSibling(outFile.getFileName() + ".tmp");
                Files.createDirectories(outFile.getParent());
                Files.write(tmp, bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                try {
                    Files.move(tmp, outFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(tmp, outFile, StandardCopyOption.REPLACE_EXISTING);
                }
                log.info("Sync por snapshot: index.jsonl v{} ({} bytes)", leaderVersion, bytes.length);
                pendingPrepare.clear();

                currentCids.clear();
                currentCids.addAll(readLocalCids());
                currentVersion = localVersion();
            } catch (Exception e) {
                log.warn("Falha no snapshot por heartbeat: {}", e.toString());
            }
        }
    }

    private List<SearchMessages.SearchItem> runLocalSearch(String prompt, int topK) {
        try {
            var items = inMemoryIndex.search(prompt, topK);
            log.info("runLocalSearch: {} resultados para '{}'", items.size(), prompt);
            return items;
        } catch (Exception e) {
            log.warn("runLocalSearch falhou: {}", e.toString());
            return List.of();
        }
    }

    public void run() throws Exception {
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "leader-hb-monitor");
            t.setDaemon(true);
            return t;
        });

        scheduler.scheduleAtFixedRate(() -> {
            try {
                var hb = new HashMap<String,Object>();
                hb.put("kind","worker_hb");
                hb.put("peer_id", peerId);
                hb.put("load", 0);
                hb.put("ts", System.currentTimeMillis());
                publishJsonToTopic(topic, mapper.writeValueAsBytes(mapper.valueToTree(hb)));
            } catch (Exception e) {
                log.warn("Falha a enviar worker_hb: {}", e.toString());
            }
        }, 1000, 3000, TimeUnit.MILLISECONDS);

        int backoff = 2;
        for (;;) {
            try (PubSubClient ipfs = new PubSubClient(apiBase);
                 Response resp = ipfs.subscribe(topic)) {

                backoff = 2;
                BufferedSource src = resp.body().source();

                for (;;) {
                    String line = src.readUtf8LineStrict();
                    if (line == null || line.isBlank()) continue;

                    try {
                        SubMsg env = mapper.readValue(line, SubMsg.class);
                        byte[] payload = decodeIpfsData(env.data);
                        if (payload.length == 0) { log.debug("Mensagem sem payload."); continue; }

                        String text = new String(payload, StandardCharsets.UTF_8).trim();
                        if (text.isBlank()) continue;
                        if ("ping".equalsIgnoreCase(text) || text.startsWith("ping")) { log.debug("Ping."); continue; }

                        JsonNode msg = mapper.readTree(text);
                        String kind = msg.path("kind").asText("");

                        switch (kind) {
                            case "prepare"      -> handlePrepare(msg);
                            case "commit"       -> handleCommit(msg);
                            case "hb" -> {
                                long leaderVersion = msg.path("version").asLong(0L);
                                String indexCid    = msg.path("index_cid").asText(null);
                                handleHeartbeat(leaderVersion, indexCid);
                            }
                            case "vote_req"     -> { }
                            case "vote_resp"    -> { }
                            case "worker_hb"    -> { }
                            case "index_update" -> {
                                Files.writeString(outFile, text + System.lineSeparator(), StandardOpenOption.APPEND);
                                log.info("IndexUpdate recebido (legacy): {}", msg.path("cid").asText(""));
                            }
                            case "ack" -> { /* ignorar acks de outros */ }
                            case "search_req" -> {
                                String target = msg.path("target_peer").asText("");
                                if (!target.isBlank() && !peerId.equals(target)) break;
                                String jobId = msg.path("job_id").asText();
                                String prompt = msg.path("prompt").asText("");
                                int topK = msg.path("top_k").asInt(5);
                                try {
                                    var items = runLocalSearch(prompt, topK);
                                    localSearchResults.put(jobId, items);

                                    var res = new HashMap<String,Object>();
                                    res.put("kind","search_result");
                                    res.put("job_id", jobId);
                                    res.put("worker_id", peerId);
                                    res.put("items", items);
                                    res.put("ts", System.currentTimeMillis());
                                    publishJsonToTopic(topic, mapper.writeValueAsBytes(mapper.valueToTree(res)));
                                    log.info("search_result publicado: job={} items={}", jobId, items.size());
                                } catch (Exception e) {
                                    var res = new HashMap<String,Object>();
                                    res.put("kind","search_result");
                                    res.put("job_id", jobId);
                                    res.put("worker_id", peerId);
                                    res.put("items", List.of());
                                    res.put("ts", System.currentTimeMillis());
                                    publishJsonToTopic(topic, mapper.writeValueAsBytes(mapper.valueToTree(res)));
                                    log.warn("Falha a executar pesquisa: {} job={}", e.toString(), jobId);
                                }
                            }
                            default -> log.warn("Mensagem desconhecida 'kind={}', payload={}", kind, text);
                        }
                    } catch (Exception perLine) {
                        log.error("Falha a processar mensagem PubSub: {}", perLine.toString());
                    }
                }

            } catch (Exception subEx) {
                log.error("Erro na subscrição PubSub: {}. Retry em {}s", subEx.toString(), backoff);
                TimeUnit.SECONDS.sleep(backoff);
                backoff = Math.min(backoff * 2, 32);
            }
        }
    }
}
