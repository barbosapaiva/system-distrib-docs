package io.sd.processor.ipfs;

import okhttp3.*;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

public class IpfsClient implements Closeable {

    private final OkHttpClient http;
    private final HttpUrl base;
    private static final MediaType BIN = MediaType.parse("application/octet-stream");

    public IpfsClient(String apiBaseUrl) {
        this.http = new OkHttpClient.Builder()
                .callTimeout(Duration.ZERO)   // stream infinito
                .readTimeout(Duration.ZERO)   // stream infinito
                .retryOnConnectionFailure(true)
                .build();
        this.base = HttpUrl.parse(apiBaseUrl);
        if (this.base == null) throw new IllegalArgumentException("URL inválido para IPFS API: " + apiBaseUrl);
    }

    private static String topicToMultibase(String topicUtf8) {
        String b64 = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(topicUtf8.getBytes(StandardCharsets.UTF_8));
        return "u" + b64;
    }

    public Response subscribe(String topic) throws Exception {
        String topicMb = topicToMultibase(topic);

        HttpUrl url = base.newBuilder()
                .addPathSegments("pubsub/sub")
                .addQueryParameter("arg", topicMb)
                .build();

        Request getReq = new Request.Builder().url(url).get().build();
        Response resp = http.newCall(getReq).execute();
        if (resp.code() == 405) {
            resp.close();
            Request postReq = new Request.Builder()
                    .url(url)
                    .post(RequestBody.create(new byte[0], BIN))
                    .build();
            resp = http.newCall(postReq).execute();
        }

        if (!resp.isSuccessful()) {
            String body = null;
            try { body = resp.body() != null ? resp.body().string() : null; } catch (Exception ignore) {}
            if (resp.body() != null) resp.close();
            throw new IllegalStateException("Falha ao subscrever: HTTP " + resp.code()
                    + (body != null ? " body=" + body : ""));
        }
        return resp; // devolve o stream (NDJSON)
    }

    @Override
    public void close() {
        http.connectionPool().evictAll();
        http.dispatcher().executorService().shutdown();
    }
}