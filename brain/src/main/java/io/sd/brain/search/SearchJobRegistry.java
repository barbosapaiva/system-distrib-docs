// src/main/java/io/sd/brain/search/SearchJobRegistry.java
package io.sd.brain.search;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;

@Component
public class SearchJobRegistry {

    private static final Logger log = LoggerFactory.getLogger(SearchJobRegistry.class);

    public static final long DEFAULT_TIMEOUT_MS = 3000;

    private static class Job {
        final String id;
        final int topK;
        final int expectedWorkers;
        final long deadlineMs;
        final CompletableFuture<List<SearchMessages.SearchItem>> future =
                new CompletableFuture<>();
        final List<SearchMessages.SearchItem> allItems = new ArrayList<>();
        int responses = 0;

        Job(String id, int topK, int expectedWorkers, long timeoutMs) {
            this.id = id;
            this.topK = topK;
            this.expectedWorkers = Math.max(1, expectedWorkers);
            long now = System.currentTimeMillis();
            this.deadlineMs = now + (timeoutMs <= 0 ? DEFAULT_TIMEOUT_MS : timeoutMs);
        }
    }

    private final ConcurrentMap<String, Job> jobs = new ConcurrentHashMap<>();

    public String createJob(int topK, int expectedWorkers, long timeoutMs) {
        String id = UUID.randomUUID().toString();
        Job j = new Job(id, topK, expectedWorkers, timeoutMs);
        jobs.put(id, j);
        log.debug("Search job criado: id={} topK={} expectedWorkers={}", id, topK, expectedWorkers);
        return id;
    }

    public void complete(String jobId, List<SearchMessages.SearchItem> items) {
        Job j = jobs.get(jobId);
        if (j == null) {
            log.debug("complete() ignorado - job {} desconhecido", jobId);
            return;
        }

        synchronized (j) {
            if (j.future.isDone()) return;

            j.responses++;
            if (items != null && !items.isEmpty()) {
                j.allItems.addAll(items);
            }

            boolean enoughResponses = (j.responses >= j.expectedWorkers);
            boolean expired = (System.currentTimeMillis() >= j.deadlineMs);

            if (enoughResponses || expired) {
                List<SearchMessages.SearchItem> merged = mergeAndTopK(j.allItems, j.topK);
                j.future.complete(merged);
                jobs.remove(jobId);
                log.info("Job {} concluído: responses={} mergedItems={}",
                        jobId, j.responses, merged.size());
            }
        }
    }

    public List<SearchMessages.SearchItem> awaitResults(String jobId, long maxWaitMs) {
        Job j = jobs.get(jobId);
        if (j == null) {
            log.debug("awaitResults() - job {} não encontrado", jobId);
            return List.of();
        }

        long now = System.currentTimeMillis();
        long remainingByDeadline = j.deadlineMs - now;
        long waitMs = maxWaitMs > 0
                ? Math.min(maxWaitMs, remainingByDeadline)
                : remainingByDeadline;

        waitMs = Math.max(1, waitMs);

        try {
            return j.future.get(waitMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.warn("Timeout à espera de resultados do job {} - devolve parciais {} items",
                    jobId, j.allItems.size());
            j.future.complete(j.allItems);
            jobs.remove(jobId);
            return j.allItems;
        } catch (Exception e) {
            log.warn("Erro em awaitResults para job {}: {}", jobId, e.toString());
            jobs.remove(jobId);
            return List.of();
        }
    }

    private List<SearchMessages.SearchItem> mergeAndTopK(List<SearchMessages.SearchItem> in, int topK) {
        if (in == null || in.isEmpty()) return List.of();

        Map<String, SearchMessages.SearchItem> bestByCid = new HashMap<>();
        for (SearchMessages.SearchItem it : in) {
            if (it == null || it.cid == null || it.cid.isBlank()) continue;
            SearchMessages.SearchItem prev = bestByCid.get(it.cid);
            if (prev == null || it.score > prev.score) {
                bestByCid.put(it.cid, it);
            }
        }

        List<SearchMessages.SearchItem> all = new ArrayList<>(bestByCid.values());
        all.sort(Comparator.comparingDouble((SearchMessages.SearchItem s) -> s.score).reversed());
        if (topK > 0 && all.size() > topK) {
            return all.subList(0, topK);
        }
        return all;
    }
}