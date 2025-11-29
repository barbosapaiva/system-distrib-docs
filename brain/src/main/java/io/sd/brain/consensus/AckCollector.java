package io.sd.brain.consensus;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class AckCollector {
    private final long version;
    private final int quorum;
    private final Map<String, String> hashesPorPeer = new ConcurrentHashMap<>();
    private volatile String agreedHash = null;

    public AckCollector(long version, int quorum) {
        this.version = version;
        this.quorum = quorum;
    }

    public long version() { return version; }
    public int quorum() { return quorum; }

    public synchronized boolean onAck(String peerId, String hash) {
        hashesPorPeer.put(peerId, hash);
        if (agreedHash == null) agreedHash = hash;
        notifyAll();
        return hasMajorityConsistent();
    }

    public boolean awaitMajoritySameHash(Duration timeout) {
        long end = System.nanoTime() + timeout.toNanos();
        synchronized (this) {
            while (!hasMajorityConsistent()) {
                long left = end - System.nanoTime();
                if (left <= 0) return false;
                try {
                    TimeUnit.NANOSECONDS.timedWait(this, left);
                } catch (InterruptedException ignored) { return false; }
            }
            return true;
        }
    }

    public synchronized String getAgreedHash() {
        return hasMajorityConsistent() ? agreedHash : null;
    }

    private boolean hasMajorityConsistent() {
        if (hashesPorPeer.size() < quorum) return false;
        if (agreedHash == null) return false;
        for (String h : hashesPorPeer.values()) {
            if (!agreedHash.equals(h)) return false;
        }
        return true;
    }


}