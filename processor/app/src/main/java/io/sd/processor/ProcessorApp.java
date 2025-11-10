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
import java.util.Map;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class ProcessorApp {

    private static final Logger log = LoggerFactory.getLogger(ProcessorApp.class);

    private final String apiBase;            // ex: http://127.0.0.1:5002/api/v0
    private final String topic;              // ex: sd-index
    private final Path outFile;              // ex: data/index.jsonl
    private final ObjectMapper mapper;
    private final OkHttpClient http;

    private final String peerId;             // env PEER_ID ou hostname
    private final Map<Long, JsonNode> pendingPrepare = new ConcurrentHashMap<>();

    public ProcessorApp() throws IOException {
        this.apiBase = System.getProperty("ipfs.api",
                System.getenv().getOrDefault("IPFS_API", "http://127.0.0.1:5002/api/v0"));
        this.topic = System.getProperty("pubsub.topic",
                System.getenv().getOrDefault("PUBSUB_TOPIC", "sd-index"));
        String out = System.getProperty("out.file",
                System.getenv().getOrDefault("OUT_FILE", "data/index.jsonl"));
        this.outFile = Paths.get(out);

        Files.createDirectories(this.outFile.getParent() == null ? Paths.get(".") : this.outFile.getParent());
        if (!Files.exists(this.outFile)) {
            Files.createFile(this.outFile);
        }

        this.mapper = new ObjectMapper();
        this.http = new OkHttpClient.Builder()
                .callTimeout(Duration.ofMinutes(1))
                .readTimeout(Duration.ofMinutes(1))
                .retryOnConnectionFailure(true)
                .build();

        String host = "peer";
        try { host = InetAddress.getLocalHost().getHostName(); } catch (Exception ignored) {}
        this.peerId = System.getenv().getOrDefault("PEER_ID", host);

        log.info("Processor configurado: api={}, topic={}, out={}", apiBase, topic, outFile);
    }

    /** Estrutura das linhas NDJSON que o Kubo envia em /pubsub/sub */
    static final class SubMsg {
        public String from;
        public String data;
        public String seqno;
        public List<String> topicIDs;
    }

    /** Decoder tolerante para o campo 'data' do pubsub (base64 vs base64url + padding). */
    private static byte[] decodeIpfsData(String b64) {
        if (b64 == null) return new byte[0];
        String s = b64.trim();

        // alguns envelopes podem vir com prefixo multibase 'u'
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

    /** 'u' + base64url( tópico UTF-8 ) sem padding — exigido pelo /pubsub/pub. */
    private static String topicToMultibase(String topicUtf8) {
        return "u" + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(topicUtf8.getBytes(StandardCharsets.UTF_8));
    }

    /** Calcula hash determinístico para confirmar a versão proposta. */
    private static String computeHash(long prevVersion, String cid) {
        try {
            String seed = prevVersion + "|" + cid;
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            byte[] dig = sha.digest(seed.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(dig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Publica JSON no /pubsub/pub com multipart "data". */
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

    /** Trata mensagens PREPARE. Guarda temporário e devolve ACK. */
    private void handlePrepare(JsonNode msg) {
        long version = msg.path("version").asLong();
        long prev = msg.path("prev_version").asLong();
        String cid = msg.path("cid").asText(null);

        if (cid == null) {
            log.warn("prepare sem 'cid', ignorado.");
            return;
        }

        pendingPrepare.put(version, msg);

        String hash = computeHash(prev, cid);

        ObjectNode ack = mapper.createObjectNode();
        ack.put("kind", "ack");
        ack.put("version", version);
        ack.put("peer_id", peerId);
        ack.put("status", "ok");
        ack.put("hash", hash);

        try {
            publishJsonToTopic(topic, mapper.writeValueAsBytes(ack));
            log.info("ACK enviado: version={} hash={}", version, hash);
        } catch (Exception e) {
            log.error("Falha a publicar ACK: {}", e.toString());
        }
    }

    /** Trata mensagens COMMIT. Valida hash e persiste linha NDJSON. */
    private void handleCommit(JsonNode msg) {
        long version = msg.path("version").asLong();
        String expectedHash = msg.path("hash").asText(null);

        JsonNode prep = pendingPrepare.remove(version);
        if (prep == null) {
            log.warn("commit v{} sem prepare previamente guardado — ignorado.", version);
            return;
        }

        long prev = prep.path("prev_version").asLong();
        String cid = prep.path("cid").asText();
        String localHash = computeHash(prev, cid);

        if (expectedHash != null && !expectedHash.equals(localHash)) {
            log.warn("Hash mismatch no commit v{} (esperado={}, local={}) — não persiste.", version, expectedHash, localHash);
            return;
        }

        ObjectNode line = mapper.createObjectNode();
        line.put("cid", cid);
        line.put("ts", System.currentTimeMillis());
        line.put("name", prep.path("name").asText(""));
        line.put("version", version);
        line.put("vector_dim", prep.path("vector_dim").asInt(0));
        line.set("vector", prep.path("vector"));
        line.put("kind", "index_update");

        try {
            Files.writeString(
                    outFile,
                    mapper.writeValueAsString(line) + System.lineSeparator(),
                    StandardOpenOption.APPEND
            );
            log.info("Commit aplicado: version={} cid={}", version, cid);
        } catch (IOException e) {
            log.error("Falha a escrever index.jsonl: {}", e.toString());
        }
    }

    /** Loop principal: subscreve com retry exponencial e processa cada linha do stream. */
    public void run() throws Exception {
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
                        if (payload.length == 0) {
                            log.warn("Mensagem sem payload (data vazio).");
                            continue;
                        }

                        String text = new String(payload, StandardCharsets.UTF_8).trim();
                        if (text.isBlank()) {
                            log.warn("Payload em branco.");
                            continue;
                        }
                        if ("ping".equalsIgnoreCase(text) || text.startsWith("ping")) {
                            log.info("Ping recebido.");
                            continue;
                        }

                        JsonNode msg = mapper.readTree(text);
                        String kind = msg.path("kind").asText("");

                        switch (kind) {
                            case "prepare" -> handlePrepare(msg);
                            case "commit"  -> handleCommit(msg);
                            case "index_update" -> { // compat: sprint 2 direto
                                Files.writeString(outFile, text + System.lineSeparator(), StandardOpenOption.APPEND);
                                log.info("IndexUpdate recebido (legacy): {}", msg.path("cid").asText(""));
                            }
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