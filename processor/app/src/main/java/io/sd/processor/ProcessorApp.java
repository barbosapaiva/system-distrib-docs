package io.sd.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.sd.processor.ipfs.IpfsClient;
import io.sd.processor.model.IndexUpdate;
import okhttp3.Response;
import okio.BufferedSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ProcessorApp {

    private static final Logger log = LoggerFactory.getLogger(ProcessorApp.class);

    private final String apiBase;       // ex: http://127.0.0.1:5002/api/v0
    private final String topic;         // ex: sd-index
    private final Path outFile;         // ex: data/index.jsonl
    private final ObjectMapper mapper;

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

        log.info("Processor configurado: api={}, topic={}, out={}", apiBase, topic, outFile);
    }

    /** Estrutura das linhas NDJSON que o Kubo envia em /pubsub/sub */
    static final class SubMsg {
        public String from;
        public String data;              // Base64 (ou base64url) do payload
        public String seqno;
        public List<String> topicIDs;
    }

    /** Decoder tolerante para o campo 'data' do pubsub (base64 vs base64url + padding). */
    private static byte[] decodeIpfsData(String b64) {
        if (b64 == null) return new byte[0];
        String s = b64.trim();

        // Se vier com multibase 'u' (usámos isso nos tópicos), tratar como base64url.
        boolean isMbUrl = s.startsWith("u");
        if (isMbUrl) s = s.substring(1);

        // Normalizar padding para múltiplos de 4
        int rem = s.length() % 4;
        if (rem == 2) s += "==";
        else if (rem == 3) s += "=";

        try {
            return (isMbUrl ? Base64.getUrlDecoder() : Base64.getDecoder()).decode(s);
        } catch (IllegalArgumentException e) {
            // Fallback: trocar '-'/'_' por '+'/'/' e voltar a tentar
            String alt = s.replace('-', '+').replace('_', '/');
            int r = alt.length() % 4;
            if (r == 2) alt += "==";
            else if (r == 3) alt += "=";
            return Base64.getDecoder().decode(alt);
        }
    }

    /** Loop principal: subscreve com retry exponencial e processa cada linha do stream. */
    public void run() throws Exception {
        int backoff = 2; // segundos
        while (true) {
            try (IpfsClient ipfs = new IpfsClient(apiBase);
                 Response resp = ipfs.subscribe(topic)) {

                // Conseguiu subscrever — reset ao backoff
                backoff = 2;

                BufferedSource src = resp.body().source();
                // Lê o stream infinito, uma linha por mensagem
                while (true) {
                    String line = src.readUtf8LineStrict(); // bloqueia até chegar uma linha
                    if (line == null || line.isBlank()) continue;

                    try {
                        SubMsg msg = mapper.readValue(line, SubMsg.class);

                        byte[] payload = decodeIpfsData(msg.data);
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

                        try {
                            IndexUpdate upd = mapper.readValue(text, IndexUpdate.class);
                            Files.writeString(
                                    outFile,
                                    text + System.lineSeparator(),
                                    StandardOpenOption.APPEND
                            );

                            try {
                                log.info("IndexUpdate recebido: version={} cid={}", upd.version(), upd.cid());
                            } catch (Throwable ignored) {
                                log.info("IndexUpdate recebido (campos não lidos).");
                            }
                        } catch (Exception parseEx) {
                            log.warn("Payload não é IndexUpdate JSON. Conteúdo: {}", text);
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