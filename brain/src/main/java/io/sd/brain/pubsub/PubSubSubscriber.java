package io.sd.brain.pubsub;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.sd.brain.cluster.NodeRoleManager;
import io.sd.brain.consensus.AckService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import okhttp3.*;
import okio.BufferedSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

/**
 * Subscreve ao tópico PubSub do IPFS e trata mensagens recebidas.
 * - Lê linhas NDJSON do endpoint /pubsub/sub
 * - Extrai o "data" (base64/base64url), decodifica e processa o JSON interno
 * - Para "kind":"ack", regista no AckService (versão, peer_id, hash)
 */
public class PubSubSubscriber {

    private static final Logger log = LoggerFactory.getLogger(PubSubSubscriber.class);

    private final String ipfsApiBase;
    private final String topic;
    private final AckService ackService;
    private final ObjectMapper om = new ObjectMapper();
    private final ClusterState cluster;
    private final NodeRoleManager roles;

    private final OkHttpClient http = new OkHttpClient.Builder()
            .readTimeout(Duration.ofMinutes(0))
            .callTimeout(Duration.ofMinutes(0))
            .retryOnConnectionFailure(true)
            .build();

    private volatile boolean running = true;
    private Thread thread;

    public PubSubSubscriber(String ipfsApi, String topic, AckService ackService,
                            ClusterState cluster, NodeRoleManager roles) {
        this.ipfsApiBase = ipfsApi.endsWith("/api/v0") ? ipfsApi : ipfsApi + "/api/v0";
        this.topic = topic;
        this.ackService = ackService;
        this.cluster = cluster;
        this.roles = roles;
    }

    @PostConstruct
    public void start() {
        thread = new Thread(this::loop, "ipfs-pubsub-sub");
        thread.setDaemon(true);
        thread.start();
        log.info("PubSubSubscriber iniciado: base={} topic={}", ipfsApiBase, topic);
    }

    @PreDestroy
    public void stop() {
        running = false;
        if (thread != null) thread.interrupt();
        log.info("PubSubSubscriber parado.");
    }

    private void loop() {
        int backoff = 2;
        while (running) {
            try {
                subscribeOnce();
                backoff = 2;
            } catch (Exception e) {
                if (!running) break;
                log.warn("Subscriber erro: {}. Retry em {}s", e.toString(), backoff);
                try { TimeUnit.SECONDS.sleep(backoff); } catch (InterruptedException ignored) {}
                backoff = Math.min(backoff * 2, 30);
            }
        }
    }

    static String encodeTopic(String topic) {
        return "u" + java.util.Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(topic.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private Response openSubscription() throws IOException {
        String encTopic = encodeTopic(topic);

        HttpUrl url = HttpUrl.parse(ipfsApiBase)
                .newBuilder()
                .addPathSegments("pubsub/sub")
                .addQueryParameter("arg", encTopic)
                .build();

        Request req = new Request.Builder()
                .url(url)
                .post(RequestBody.create(new byte[0], null))
                .build();

        Response resp = http.newCall(req).execute();
        if (!resp.isSuccessful()) {
            String body = resp.body() != null ? resp.body().string() : null;
            if (resp.body() != null) resp.close();
            throw new IllegalStateException("HTTP " + resp.code() + (body != null ? " body="+body : ""));
        }
        return resp;
    }

    private void subscribeOnce() throws Exception {
        try (Response resp = openSubscription()) {
            BufferedSource src = resp.body().source();
            while (running) {
                String line = src.readUtf8LineStrict();
                if (line == null || line.isBlank()) continue;

                try {
                    JsonNode env = om.readTree(line);
                    String b64 = env.path("data").asText(null);
                    if (b64 == null) {
                        log.debug("Linha sem campo 'data': {}", line);
                        continue;
                    }

                    byte[] payload = decodeIpfsData(b64);
                    if (payload.length == 0) {
                        log.debug("Payload vazio.");
                        continue;
                    }

                    String text = new String(payload, StandardCharsets.UTF_8).trim();
                    if (text.isBlank()) continue;
                    if ("ping".equalsIgnoreCase(text) || text.startsWith("ping")) {
                        log.debug("Ping recebido do PubSub.");
                        continue;
                    }

                    JsonNode msg = om.readTree(text);
                    String kind = msg.path("kind").asText("");

                    switch (kind) {
                        case "ack" -> {
                            if (!roles.isLeader()) break; // só o líder precisa registar ACKs
                            long version = msg.path("version").asLong(0);
                            String peerId = msg.path("peer_id").asText("");
                            String status = msg.path("status").asText("ok");
                            String hash = msg.path("hash").asText("");

                            if (!"ok".equalsIgnoreCase(status) || version <= 0 || hash.isBlank()) {
                                log.warn("ACK inválido: {}", text);
                                break;
                            }
                            boolean maj = ackService.register(version, peerId, hash);
                            log.info("ACK registado: version={} peer={} hash={} (maioria? {})",
                                    version, peerId, hash, maj);
                        }
                        case "prepare" -> {
                            log.debug("Prepare visto (subscriber) - sem ação aqui.");
                        }
                        case "commit" -> {
                            log.debug("Commit visto (subscriber) - sem ação aqui.");
                        }
                        case "hb" -> {
                            long term = msg.path("term").asLong(0L);
                            String id = msg.path("id").asText("");
                            long ts = msg.path("ts").asLong(System.currentTimeMillis());
                            long v = msg.path("version").asLong(0L);
                            String idx = msg.path("index_cid").asText(null);

                            roles.observeHeartbeat(term, id, ts);
                            cluster.setTermAndLeader(term, id);
                            if (v > 0 && idx != null && !idx.isBlank()) {
                                cluster.setVersionAndIndexCid(v, idx);
                            }
                            log.debug("Heartbeat recebido: term={} leader={} version={} idx={}", term, id, v, idx);
                        }
                        default -> log.warn("Mensagem desconhecida kind={}, payload={}", kind, text);
                    }
                } catch (Exception perLine) {
                    log.error("Falha a processar linha PubSub: {}", perLine.toString());
                }
            }
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
}