package io.sd.brain.index;

import org.springframework.stereotype.Service;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class VersionVectorService {
    private final List<String> committed = new ArrayList<>();
    private final AtomicInteger currentVersion = new AtomicInteger(0);

    public synchronized IndexUpdate buildCandidate(String cid, String name, int vectorDim, float[] vector) {
        int next = currentVersion.get() + 1;
        List<String> candidate = new ArrayList<>(committed);
        candidate.add(cid);
        String cidsHash = sha256(String.join("|", candidate));
        return new IndexUpdate("INDEX_UPDATE", next, cid, name, vectorDim, vector, cidsHash, System.currentTimeMillis()/1000);
    }

    public synchronized void commit(IndexUpdate upd) {
        committed.add(upd.cid());
        currentVersion.set(upd.version());
    }

    public int currentVersion() { return currentVersion.get(); }

    private static String sha256(String s) {
        try {
            var md = MessageDigest.getInstance("SHA-256");
            byte[] d = md.digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder("sha256:");
            for (byte b : d) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    public static class IndexUpdate {
        private final String type;
        private final int version;
        private final String cid;
        private final String name;
        private final int vector_dim;
        private final float[] vector;
        private final String cids_hash;
        private final long ts;

        public IndexUpdate(String type, int version, String cid, String name,
                           int vector_dim, float[] vector, String cids_hash, long ts) {
            this.type = type;
            this.version = version;
            this.cid = cid;
            this.name = name;
            this.vector_dim = vector_dim;
            this.vector = vector;
            this.cids_hash = cids_hash;
            this.ts = ts;
        }
        public String type() { return type; }
        public int version() { return version; }
        public String cid() { return cid; }
        public String name() { return name; }
        public int vector_dim() { return vector_dim; }
        public float[] vector() { return vector; }
        public String cids_hash() { return cids_hash; }
        public long ts() { return ts; }
    }
}