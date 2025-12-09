package io.sd.brain.search;

import io.sd.brain.emb.EmbeddingService;
import ai.djl.translate.TranslateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryIndex {

    private static final Logger log = LoggerFactory.getLogger(InMemoryIndex.class);

    private final EmbeddingService embedService;

    private final Map<String, float[]> vectorsByCid = new ConcurrentHashMap<>();
    private final Map<String, String> namesByCid   = new ConcurrentHashMap<>();

    public InMemoryIndex(EmbeddingService embedService) {
        this.embedService = embedService;
    }

    public void addOrUpdate(String cid, float[] vector) {
        if (cid == null || vector == null) return;
        vectorsByCid.put(cid, vector);
    }

    public void setName(String cid, String name) {
        if (cid == null) return;
        namesByCid.put(cid, name == null ? "" : name);
    }

    public List<SearchMessages.SearchItem> search(String prompt, int topK) {
        if (prompt == null || prompt.isBlank()) {
            return List.of();
        }

        float[] qVec;
        try {
            qVec = embedService.embed(prompt);
        } catch (TranslateException e) {
            log.warn("Falha a gerar embedding para query '{}': {}", prompt, e.toString());
            return List.of();
        }
        if (qVec == null || qVec.length == 0) {
            return List.of();
        }

        normalizeInPlace(qVec);

        List<SearchMessages.SearchItem> out = new ArrayList<>();

        for (var entry : vectorsByCid.entrySet()) {
            String cid = entry.getKey();
            float[] v = entry.getValue();
            if (v == null || v.length != qVec.length) continue;

            double score = cosineSim(qVec, v);

            SearchMessages.SearchItem item = new SearchMessages.SearchItem();
            item.cid = cid;
            item.name = namesByCid.getOrDefault(cid, "");
            item.score = score;
            out.add(item);
        }

        out.sort((a, b) -> Double.compare(b.score, a.score));

        if (out.size() > topK) {
            return new ArrayList<>(out.subList(0, topK));
        }
        return out;
    }

    private static void normalizeInPlace(float[] v) {
        double norm2 = 0.0;
        for (float x : v) norm2 += (double) x * x;
        if (norm2 == 0.0) return;
        double inv = 1.0 / Math.sqrt(norm2);
        for (int i = 0; i < v.length; i++) {
            v[i] = (float) (v[i] * inv);
        }
    }

    private static double cosineSim(float[] a, float[] b) {
        double dot = 0.0;
        double na = 0.0;
        double nb = 0.0;
        for (int i = 0; i < a.length; i++) {
            double x = a[i];
            double y = b[i];
            dot += x * y;
            na += x * x;
            nb += y * y;
        }
        if (na == 0 || nb == 0) return 0.0;
        return dot / (Math.sqrt(na) * Math.sqrt(nb));
    }
}