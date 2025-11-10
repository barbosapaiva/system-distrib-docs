package io.sd.processor.store;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class PeerState {
    long currentVersion = 0L;
    List<String> currentCids = new ArrayList<>();

    static class Pending {
        long prevVersion;
        String newCid;
        int vectorDim;
        float[] vector;         // buffer embeddings até commit
    }
    Map<Long, Pending> pending = new ConcurrentHashMap<>();
}