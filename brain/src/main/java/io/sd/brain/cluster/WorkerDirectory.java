package io.sd.brain.cluster;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkerDirectory {

    public static final class WorkerInfo {
        public final String id;
        public volatile long lastSeen;
        public volatile int load;

        WorkerInfo(String id) { this.id = id; this.lastSeen = System.currentTimeMillis(); }
    }

    private final ConcurrentMap<String, WorkerInfo> workers = new ConcurrentHashMap<>();
    private final AtomicInteger rr = new AtomicInteger();

    public void upsert(String id, Integer load) {
        WorkerInfo w = workers.computeIfAbsent(id, WorkerInfo::new);
        w.lastSeen = System.currentTimeMillis();
        if (load != null) w.load = load;
    }
}