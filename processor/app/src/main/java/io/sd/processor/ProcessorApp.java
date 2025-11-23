package io.sd.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.sd.processor.ipfs.IpfsClient;
import okhttp3.*;
import okio.BufferedSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class ProcessorApp {

    private static final Logger log = LoggerFactory.getLogger(ProcessorApp.class);

    private final String apiBase;            // ex: http://127.0.0.1:5004/api/v0
    private final String topic;              // ex: sd-index
    private final Path outFile;              // ex: data/index-5004.jsonl
    private final ObjectMapper mapper;
    private final OkHttpClient http;

    private final String peerId;             // env PEER_ID ou hostname

    // Estado transitório e em memória
    private final Map<Long, JsonNode> pendingPrepare = new ConcurrentHashMap<>();
    private final Map<Long, float[]> pendingVectors = new ConcurrentHashMap<>();
    private final List<String> currentCids = new java.util.concurrent.CopyOnWriteArrayList<>();
    private volatile long currentVersion = 0L;

    // Fail-stop detection
    private final long hbTimeoutMs = Long.parseLong(System.getProperty("leader.timeout.ms", System.getenv().getOrDefault("LEADER_TIMEOUT_MS","10000"))); // p.ex. 10s
    private final AtomicLong lastHbTs = new AtomicLong(0);
    private volatile boolean leaderAlive = false;

    public ProcessorApp() throws IOException {
        this.apiBase = System.getProperty("ipfs.api",
                System.getenv().getOrDefault("IPFS_API", "http://127.0.0.1:5003/api/v0"));
        this.topic = System.getProperty("pubsub.topic",
                System.getenv().getOrDefault("PUBSUB_TOPIC", "sd-index"));
        String out = System.getProperty("out.file",
                System.getenv().getOrDefault("OUT_FILE", "data/index-5003.jsonl"));
        this.outFile = Paths.get(out);

        Files.createDirectories(this.outFile.getParent() == null ? Paths.get(".") : this.outFile.getParent());
        if (!Files.exists(this.outFile)) Files.createFile(this.outFile);

        this.mapper = new ObjectMapper();
        this.http = new OkHttpClient.Builder()
                .callTimeout(Duration.ofMinutes(1))
                .readTimeout(Duration.ofMinutes(1))
                .retryOnConnectionFailure(true)
                .build();

        String host = "peer";
        try { host = InetAddress.getLocalHost().getHostName(); } catch (Exception ignored) {}
        this.peerId = System.getenv().getOrDefault("PEER_ID", host);

        // Inicializa estado em memória a partir do ficheiro, se existir
        try {
            this.currentCids.clear();
            this.currentCids.addAll(readLocalCids());
            this.currentVersion = localVersion();
        } catch (Exception ignored) {}

        log.info("Processor configurado: api={}, topic={}, out={}", apiBase, topic, outFile);
    }

    /** Estrutura das linhas NDJSON que o Kubo envia em /pubsub/sub */
    static final class SubMsg {
        public String from;
        public String data;
        public String seqno;
        public List<String> topicIDs;
    }

    /* ===================== Utils de estado/ficheiro ===================== */

    private List<String> readLocalCids() {
        try {
            if (!Files.exists(outFile)) return List.of();
            var lines = Files.readAllLines(outFile);
            var out = new java.util.ArrayList<String>(lines.size());
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

    private static String sha256Hex(String prefix, String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] dig = md.digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(prefix == null ? "" : prefix);
            for (byte b : dig) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    /* ===================== Codificação PubSub/IPFS ===================== */

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

    /* ===================== Handlers ===================== */

    /** PREPARE: guarda temporário, calcula/aceita cids_hash e devolve ACK. */
    private void handlePrepare(JsonNode msg) throws IOException {
        long version = msg.path("version").asLong();
        String cid = msg.path("cid").asText(null);
        if (cid == null) { log.warn("prepare sem 'cid'"); return; }

        // cids = estado atual + novo cid (se o líder já enviou cids completos, mantém)
        List<String> cids;
        if (msg.has("cids") && msg.get("cids").isArray()) {
            cids = mapper.convertValue(msg.get("cids"), mapper.getTypeFactory()
                    .constructCollectionType(List.class, String.class));
        } else {
            cids = new java.util.ArrayList<>(currentCids);
            cids.add(cid);
            ((ObjectNode) msg).set("cids", mapper.valueToTree(cids));
        }

        // Se vier vector no prepare, guarda-o para aplicar no commit
        if (msg.has("vector")) {
            float[] vec = mapper.convertValue(msg.get("vector"), float[].class);
            pendingVectors.put(version, vec);
        }

        // Hash local e hash do líder (se existir)
        String localHash = sha256Hex("sha256:", String.join("|", cids));
        String leaderHash = msg.path("cids_hash").asText(localHash);
        if (!leaderHash.equals(localHash)) {
            log.warn("cids_hash do líder difere do local (provável desfasamento). ack com hash do líder.");
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

    /** COMMIT: valida hash e persiste linha NDJSON; promove estado e aplica vector pendente. */
    private void handleCommit(JsonNode msg) {
        long version = msg.path("version").asLong();
        String expectedHash = msg.path("hash").asText(null);

        JsonNode prep = pendingPrepare.remove(version);
        if (prep == null) {
            log.warn("commit v{} sem prepare previamente guardado — ignorado.", version);
            return;
        }

        String cidsHash = prep.path("cids_hash").asText(null);
        if (expectedHash == null || cidsHash == null || !expectedHash.equals(cidsHash)) {
            log.warn("Hash mismatch no commit v{} (esperado={}, local={}) — não persiste.", version, expectedHash, cidsHash);
            return;
        }

        String cid = prep.path("cid").asText();
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


        try {
            List<String> newCids = mapper.convertValue(prep.get("cids"),
                    mapper.getTypeFactory().constructCollectionType(List.class, String.class));
            currentCids.clear();
            currentCids.addAll(newCids);
            currentVersion = version;
        } catch (Exception ignored) {}

        float[] vec = pendingVectors.remove(version);
    }

    /** HEARTBEAT: se líder anuncia versão > local, substitui o index.jsonl via IPFS. */
    private void handleHeartbeat(JsonNode msg) {
        long leaderV = msg.path("version").asLong(0);
        String idxCid = msg.path("index_cid").asText(null);
        lastHbTs.set(System.currentTimeMillis());
        leaderAlive = true;

        if (idxCid == null || idxCid.isBlank()) return;
        long vLocal = localVersion();
        if (leaderV > vLocal) {
            try {
                byte[] bytes = ipfsCat(idxCid);
                Path tmp = outFile.resolveSibling(outFile.getFileName() + ".tmp");
                Files.createDirectories(outFile.getParent());
                Files.write(tmp, bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                try {
                    Files.move(tmp, outFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(tmp, outFile, StandardCopyOption.REPLACE_EXISTING);
                }
                log.info("Sync por snapshot: index.jsonl v{} ({} bytes)", leaderV, bytes.length);
                pendingPrepare.clear();
                pendingPrepare.clear();

                currentCids.clear();
                currentCids.addAll(readLocalCids());
                currentVersion = localVersion();
            } catch (Exception e) {
                log.warn("Falha sync por snapshot: {}", e.toString());
            }
        }
    }

    /* ===================== Loop principal ===================== */

    public void run() throws Exception {

        // Monitor de falha do líder (fail-stop via timeout de heartbeat)
        var scheduler = newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "leader-hb-monitor");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(() -> {
            long last = lastHbTs.get();
            long now = System.currentTimeMillis();
            if (leaderAlive && last > 0 && (now - last) > hbTimeoutMs) {
                leaderAlive = false;
                log.warn("⚠️  Suspeita de falha do líder: sem heartbeat há {} ms (timeout={} ms).",
                        (now - last), hbTimeoutMs);
                pendingPrepare.clear();
            }
        }, 500, 5000, TimeUnit.MILLISECONDS);

        int backoff = 2;
        while (true) {
            try (IpfsClient ipfs = new IpfsClient(apiBase);
                 Response resp = ipfs.subscribe(topic)) {

                backoff = 2;
                BufferedSource src = resp.body().source();

                while (true) {
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
                            case "prepare" -> handlePrepare(msg);
                            case "commit"  -> handleCommit(msg);
                            case "hb"      -> handleHeartbeat(msg);
                            case "index_update" -> {
                                Files.writeString(outFile, text + System.lineSeparator(), StandardOpenOption.APPEND);
                                log.info("IndexUpdate recebido (legacy): {}", msg.path("cid").asText(""));
                            }
                            case "ack" -> { /* ignorar no processor */ }
                            default -> log.warn("Mensagem desconhecida 'kind={}', payload={}", kind, text);
                        }

                    } catch (Exception e) {
                        log.error("Falha a processar mensagem PubSub: {}", e.toString());
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