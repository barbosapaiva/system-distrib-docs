package io.sd.brain.cluster;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class NodeRoleManager {

    private final String myId;
    private final AtomicLong currentTerm = new AtomicLong(0);
    private final AtomicBoolean leader = new AtomicBoolean(false);
    private volatile String leaderId = null;
    private final AtomicLong lastHbTs = new AtomicLong(0);

    public NodeRoleManager(@Value("${node.id:}") String configuredId) {
        this.myId = resolveNodeId(configuredId);
    }

    private static String resolveNodeId(String configured) {
        if (configured != null && !configured.isBlank()) return configured;
        try {
            String hn = java.net.InetAddress.getLocalHost().getHostName();
            if (hn != null && !hn.isBlank()) return hn;
        } catch (Exception ignored) {}
        String env = System.getenv("HOSTNAME");
        if (env != null && !env.isBlank()) return env;
        return "node-" + java.util.UUID.randomUUID();
    }

    public String myId() { return myId; }
    public long term() { return currentTerm.get(); }
    public boolean isLeader() { return leader.get(); }
    public String leaderId() { return leaderId; }
    public long lastHeartbeatTs() { return lastHbTs.get(); }

    // Observa heartbeats recebidos
    public synchronized void observeHeartbeat(long term, String hbLeaderId, long ts) {
        lastHbTs.set(ts > 0 ? ts : System.currentTimeMillis());
        long t = currentTerm.get();

        if (term > t) {
            currentTerm.set(term);
            leaderId = hbLeaderId;
            leader.set(myId.equals(hbLeaderId));
            return;
        }
        if (term == t) {
            // Desempate determinístico por id
            String cur = leaderId == null ? "" : leaderId;
            if (hbLeaderId != null && hbLeaderId.compareTo(cur) > 0) {
                leaderId = hbLeaderId;
                leader.set(myId.equals(hbLeaderId));
            }
        }
        // term < t - ignora
    }

    // Tentativa de promoção a líder (incrementa term)
    public synchronized boolean tryPromote() {
        long newTerm = currentTerm.incrementAndGet();
        leaderId = myId;
        leader.set(true);
        // marca como se tivesse emitido heartbeat agora
        lastHbTs.set(System.currentTimeMillis());
        return true;
    }

    // Perde liderança
    public synchronized void stepDown(long term, String newLeaderId) {
        if (term >= currentTerm.get()) currentTerm.set(term);
        leaderId = newLeaderId;
        leader.set(false);
    }
}