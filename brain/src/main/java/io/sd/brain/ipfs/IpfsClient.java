package io.sd.brain.ipfs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class IpfsClient {
    private final String apiBase; // pode vir com ou sem /api/v0
    private final ObjectMapper mapper = new ObjectMapper();
    // timeouts razoáveis (podes tornar configuráveis)
    private final int connectTimeoutMs = 10_000;
    private final int readTimeoutMs    = 60_000;

    public IpfsClient(String apiBase) {
        String base = apiBase.trim();
        if (base.endsWith("/")) base = base.substring(0, base.length() - 1);
        this.apiBase = base;
    }

    private String apiV0() {
        return apiBase.endsWith("/api/v0") ? apiBase : apiBase + "/api/v0";
    }

    public String add(String filename, byte[] bytes, boolean pin) throws IOException {
        String boundary = "----JavaForm" + UUID.randomUUID();
        String urlStr = apiV0() + "/add?pin=" + pin + "&cid-version=1&raw-leaves=true";
        var url = URI.create(urlStr);

        HttpURLConnection conn = (HttpURLConnection) url.toURL().openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("POST");
        conn.setConnectTimeout(connectTimeoutMs);
        conn.setReadTimeout(readTimeoutMs);
        conn.setRequestProperty("Accept", "application/json");
        conn.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);

        try (var out = new DataOutputStream(conn.getOutputStream())) {
            out.writeBytes("--" + boundary + "\r\n");
            out.writeBytes("Content-Disposition: form-data; name=\"file\"; filename=\"" +
                    sanitizeName(filename) + "\"\r\n");
            out.writeBytes("Content-Type: application/octet-stream\r\n\r\n");
            out.write(bytes);
            out.writeBytes("\r\n--" + boundary + "--\r\n");
        }

        int code = conn.getResponseCode();
        if (code != 200) {
            String errBody = readAll(conn.getErrorStream());
            if (errBody == null || errBody.isBlank()) errBody = conn.getResponseMessage();
            conn.disconnect();
            throw new IOException("IPFS add falhou (" + code + ") url=" + urlStr + " : " + errBody);
        }

        String cid = null;
        try (var in = new BufferedReader(
                new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = in.readLine()) != null) {
                if (line.isBlank()) continue;
                JsonNode node = mapper.readTree(line);

                if (node.has("Hash")) {
                    cid = node.get("Hash").asText();
                } else if (node.has("Cid")) {
                    String maybe = node.path("Cid").path("/").asText(null);
                    if (maybe != null && !maybe.isBlank()) cid = maybe;
                }
            }
        } finally {
            conn.disconnect();
        }

        if (cid == null || cid.isBlank()) {
            throw new IOException("CID não encontrado na resposta do IPFS (url=" + urlStr + ")");
        }
        return cid;
    }

    public byte[] cat(String cid) throws IOException {
        String enc = URLEncoder.encode(cid, StandardCharsets.UTF_8);
        var url = URI.create(apiV0() + "/cat?arg=" + enc).toURL();  // <-- usa apiV0()
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("POST");
        conn.setConnectTimeout(connectTimeoutMs);
        conn.setReadTimeout(readTimeoutMs);

        int code = conn.getResponseCode();
        if (code != 200) {
            String errBody = readAll(conn.getErrorStream());
            conn.disconnect();
            throw new IOException("IPFS cat falhou (" + code + ")" +
                    (errBody != null ? " : " + errBody : ""));
        }
        try (var in = conn.getInputStream()) {
            return in.readAllBytes();
        } finally {
            conn.disconnect();
        }
    }

    private static String sanitizeName(String name) {
        String n = name == null ? "file.bin" : name;
        int slash = Math.max(n.lastIndexOf('/'), n.lastIndexOf('\\'));
        if (slash >= 0) n = n.substring(slash + 1);
        return n.replaceAll("[\\r\\n\\t\"]", "_");
    }

    private static String readAll(InputStream is) {
        if (is == null) return null;
        try (is) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            return null;
        }
    }
}