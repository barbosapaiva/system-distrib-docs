package io.sd.brain.pubsub;

import org.springframework.stereotype.Component;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class ClusterState {
    private final AtomicLong version = new AtomicLong(0);
    private volatile String indexCid = null;

    private final AtomicLong term = new AtomicLong(0);
    private volatile String leaderId = null;

    public long getVersion() { return version.get(); }
    public String getIndexCid() { return indexCid; }

    public long getTerm() { return term.get(); }
    public String getLeaderId() { return leaderId; }

    public void setVersionAndIndexCid(long v, String cid) {
        version.set(v);
        indexCid = cid;
    }

    public void setTermAndLeader(long t, String id) {
        term.set(t);
        leaderId = id;
    }
}