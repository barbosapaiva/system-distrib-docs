package io.sd.brain.ipfs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.UUID;

/**
 * Cliente para a IPFS HTTP API (endpoint /api/v0/add).
 * Usa multipart/form-data e extrai o campo "Hash" (CID) da resposta JSON.
 */
public class IpfsClient {
    private final String apiBase;
    private final ObjectMapper mapper = new ObjectMapper();

    public IpfsClient(String apiBase) {
        this.apiBase = apiBase.endsWith("/") ? apiBase.substring(0, apiBase.length()-1) : apiBase;
    }

    public String add(String filename, byte[] bytes, boolean pin) throws IOException {
        String boundary = "----JavaForm" + UUID.randomUUID();
        var url = URI.create(apiBase + "/api/v0/add?pin=" + pin + "&cid-version=1&raw-leaves=true");
        var conn = (HttpURLConnection) url.toURL().openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);

        try (var out = new DataOutputStream(conn.getOutputStream())) {
            out.writeBytes("--" + boundary + "\r\n");
            out.writeBytes("Content-Disposition: form-data; name=\"file\"; filename=\"" + filename + "\"\r\n");
            out.writeBytes("Content-Type: application/octet-stream\r\n\r\n");
            out.write(bytes);
            out.writeBytes("\r\n--" + boundary + "--\r\n");
        }

        int code = conn.getResponseCode();
        if (code != 200) {
            try (var es = conn.getErrorStream()) {
                String msg = es != null ? new String(es.readAllBytes()) : conn.getResponseMessage();
                throw new IOException("IPFS add falhou (" + code + "): " + msg);
            }
        }

        String cid = null;
        try (var in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            String line;
            while ((line = in.readLine()) != null) {
                if (line.isBlank()) continue;
                JsonNode node = mapper.readTree(line);
                if (node.has("Hash")) {
                    cid = node.get("Hash").asText();
                }
            }
        }
        if (cid == null || cid.isBlank()) {
            throw new IOException("CID não encontrado na resposta do IPFS");
        }
        return cid;
    }
}