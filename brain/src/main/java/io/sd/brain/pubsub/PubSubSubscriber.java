package io.sd.brain.pubsub;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.sd.brain.node.NodeRoleManager;
import io.sd.brain.cluster.WorkerDirectory;
import io.sd.brain.consensus.AckService;
import io.sd.brain.search.SearchJobRegistry;
import io.sd.brain.search.SearchMessages;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import okhttp3.*;
import okio.BufferedSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Base64.*;

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
    private final WorkerDirectory workers;
    private final SearchJobRegistry jobs;
    private final PubSubService pub;
    private final int clusterQuorum;

    private final OkHttpClient http = new OkHttpClient.Builder()
            .readTimeout(Duration.ofMinutes(0))
            .callTimeout(Duration.ofMinutes(0))
            .retryOnConnectionFailure(true)
            .build();

    private volatile boolean running = true;
    private Thread thread;

    public PubSubSubscriber(String ipfsApi,
                            String topic,
                            AckService ackService,
                            ClusterState cluster,
                            NodeRoleManager roles,
                            WorkerDirectory workers,
                            SearchJobRegistry jobs,
                            PubSubService pub,
                            int clusterQuorum) {
        this.ipfsApiBase = ipfsApi.endsWith("/api/v0") ? ipfsApi : ipfsApi + "/api/v0";
        this.topic = topic;
        this.ackService = ackService;
        this.cluster = cluster;
        this.roles = roles;
        this.workers = workers;
        this.jobs = jobs;
        this.pub = pub;
        this.clusterQuorum = clusterQuorum;
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
        return "u" + getUrlEncoder()
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
//                        case "hb" -> {
//                            long term = msg.path("term").asLong(0L);
//                            String id = msg.path("id").asText("");
//                            long ts = msg.path("ts").asLong(System.currentTimeMillis());
//                            long v = msg.path("version").asLong(0L);
//                            String idx = msg.path("index_cid").asText(null);
//
//                            roles.observeHeartbeat(term, id, ts);
//                            cluster.setTermAndLeader(term, id);
//                            if (v > 0 && idx != null && !idx.isBlank()) {
//                                cluster.setVersionAndIndexCid(v, idx);
//                            }
//                            log.debug("Heartbeat recebido: term={} leader={} version={} idx={}", term, id, v, idx);
//                        }
                        case "hb" -> {
                            long hbTerm = msg.path("term").asLong();
                            String hbLeaderId = msg.path("id").asText(null);
                            long hbTs = msg.path("ts").asLong(System.currentTimeMillis());

                            log.debug("RAFT HB recebido: term={} leader={} ts={}", hbTerm, hbLeaderId, hbTs);

                            if (hbLeaderId == null || hbLeaderId.isBlank()) {
                                log.warn("Ignorar RAFT heartbeat sem leaderId: {}", text);
                                continue;
                            }

                            if (roles.myId().equals(hbLeaderId)) {
                                cluster.setTermAndLeader(hbTerm, hbLeaderId);
                                log.debug("Ignorar heartbeat do próprio líder {}", hbLeaderId);
                                continue;
                            }
                            // ------------------------------------------------------

                            roles.onHeartbeat(hbTerm, hbLeaderId, hbTs);
                            cluster.setTermAndLeader(hbTerm, hbLeaderId);
                        }
                        case "hello" -> {
                            String peer = msg.path("peer_id").asText("");
                            workers.upsert(peer, null);
                            log.info("Worker registado: {}", peer);
                        }
                        case "worker_hb" -> {
                            String peer = msg.path("peer_id").asText("");
                            int load = msg.path("load").asInt(0);
                            workers.upsert(peer, load);
                        }
                        case "vote_req" -> {
                            long term = msg.path("term").asLong();
                            String candidateId = msg.path("candidate_id").asText();
                            long ts = msg.path("ts").asLong(System.currentTimeMillis());

                          if (candidateId.equals(roles.myId())) {
                                break;
                            }

                            var decision = roles.onVoteRequest(term, candidateId, ts);

                            Map<String, Object> voteResp = new HashMap<>();
                            voteResp.put("kind", "vote_resp");
                            voteResp.put("term", decision.term);
                            voteResp.put("candidate_id", candidateId);
                            voteResp.put("voter_id", roles.myId());
                            voteResp.put("granted", decision.granted);

                            pub.publishJson(topic, voteResp);
                        }
                        case "vote_resp" -> {
                            long term = msg.path("term").asLong();
                            String candidateId = msg.path("candidate_id").asText();
                            String voterId = msg.path("voter_id").asText();
                            boolean granted = msg.path("granted").asBoolean(false);

                            if (!candidateId.equals(roles.myId())) {
                                break;
                            }

                            long now = System.currentTimeMillis();
                            boolean becameLeader = roles.onVoteResponse(term, granted, voterId, clusterQuorum, now);
                            if (becameLeader) {
                                cluster.setTermAndLeader(roles.term(), roles.myId());
                                log.warn("ClusterState atualizado: leader={} term={}", roles.myId(), roles.term());
                            }
                        }
                        case "search_result" -> {
                            String job = msg.path("job_id").asText("");
                            String worker = msg.path("worker_id").asText("");
                            var arr = msg.path("items");
                            ArrayList<SearchMessages.SearchItem> items = new ArrayList<>();
                            if (arr.isArray()) {
                                for (var it : arr) {
                                    var s = new io.sd.brain.search.SearchMessages.SearchItem();
                                    s.cid = it.path("cid").asText();
                                    s.name = it.path("name").asText("");
                                    s.score = it.path("score").asDouble(0.0);
                                    items.add(s);
                                }
                            }
                            jobs.complete(job, items);
                            log.info("Resultado de pesquisa concluído: job={} worker={} items={}", job, worker, items.size());
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
            return (isMbUrl ? getUrlDecoder() : getDecoder()).decode(s);
        } catch (IllegalArgumentException e) {
            String alt = s.replace('-', '+').replace('_', '/');
            int r = alt.length() % 4;
            if (r == 2) alt += "==";
            else if (r == 3) alt += "=";
            return getDecoder().decode(alt);
        }
    }
}