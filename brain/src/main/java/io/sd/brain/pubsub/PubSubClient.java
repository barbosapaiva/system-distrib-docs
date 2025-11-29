package io.sd.brain.pubsub;

import okhttp3.*;
import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

public class PubSubClient implements AutoCloseable {

    private final OkHttpClient http;
    private final HttpUrl base;

    public PubSubClient(String apiBaseUrl) {
        this.http = new OkHttpClient.Builder()
                .callTimeout(Duration.ZERO)   // stream infinito
                .readTimeout(Duration.ZERO)   // stream infinito
                .retryOnConnectionFailure(true)
                .build();
        String baseStr = apiBaseUrl.endsWith("/api/v0") ? apiBaseUrl : apiBaseUrl + "/api/v0";
        this.base = HttpUrl.parse(baseStr);
        if (this.base == null) throw new IllegalArgumentException("URL inválido: " + apiBaseUrl);
    }

    private static String encodeTopic(String topicUtf8) {
        return "u" + Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(topicUtf8.getBytes(StandardCharsets.UTF_8));
    }

    /** Abre o stream NDJSON do /pubsub/sub. Tens de fechar o Response no chamador. */
    public Response subscribe(String topic) throws Exception {
        String encTopic = encodeTopic(topic);
        HttpUrl url = base.newBuilder()
                .addPathSegments("pubsub/sub")
                .addQueryParameter("arg", encTopic)
                .build();

        Request req = new Request.Builder()
                .url(url)
                .post(RequestBody.create(new byte[0], null))
                .build();

        Response resp = http.newCall(req).execute();
        if (!resp.isSuccessful()) {
            String body = null;
            try { body = resp.body() != null ? resp.body().string() : null; } catch (Exception ignore) {}
            if (resp.body() != null) resp.close();
            throw new IllegalStateException("Falha subscrição: HTTP " + resp.code() +
                    (body != null ? " body=" + body : ""));
        }
        return resp; // devolve o stream
    }

    @Override
    public void close() {
        http.connectionPool().evictAll();
        http.dispatcher().executorService().shutdown();
    }
}