package io.sd.brain.pubsub;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

@Service
public class PubSubService {

    private final RestClient http = RestClient.create();
    private final ObjectMapper om = new ObjectMapper();
    private final String ipfsApiBase; // ex: http://127.0.0.1:5001/api/v0

    public PubSubService(@Value("${ipfs.api}") String ipfsApi) {
        this.ipfsApiBase = ipfsApi.endsWith("/api/v0") ? ipfsApi : ipfsApi + "/api/v0";
    }

    /** multibase 'u' + base64url (sem '=') do UTF-8 do tópico */
    private static String topicToMultibase(String topicUtf8) {
        String b64 = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(topicUtf8.getBytes(StandardCharsets.UTF_8));
        return "u" + b64;
    }

    /** Publica um JSON no tópico dado */
    public void publishJson(String topic, Object payload) {
        try {
            String topicMb = topicToMultibase(topic);
            String uri = ipfsApiBase + "/pubsub/pub?arg=" + topicMb;

            byte[] body = om.writeValueAsBytes(payload);

            MultipartBodyBuilder mb = new MultipartBodyBuilder();
            mb.part("data", body)
                    .filename("msg.json")
                    .contentType(MediaType.APPLICATION_JSON);

            http.post()
                    .uri(uri)
                    .contentType(MediaType.MULTIPART_FORM_DATA)
                    .body(mb.build())
                    .retrieve()
                    .toBodilessEntity();
        } catch (RestClientResponseException ex) {
            // Isto ajuda-te a ver o erro real do IPFS em logs
            String msg = "Falha no publish PubSub: " + ex.getRawStatusCode() + " " + ex.getStatusText()
                    + " body=" + ex.getResponseBodyAsString();
            throw new RuntimeException(msg, ex);
        } catch (Exception e) {
            throw new RuntimeException("Falha no publish PubSub", e);
        }
    }
}