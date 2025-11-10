package io.sd.brain.index;

import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Base64;

@Service
public class VersionVectorService {

    private final List<String> committed = new ArrayList<>();
    private final AtomicInteger currentVersion = new AtomicInteger(0);

    /** Cria o candidato (v = prev+1) sem confirmar a versão atual. */
    public synchronized IndexUpdate buildCandidate(String cid, String name, int vectorDim, float[] vector) {
        int prev = currentVersion.get();
        int next = prev + 1;

        List<String> candidate = new ArrayList<>(committed);
        candidate.add(cid);

        String cidsHash = sha256Hex("sha256:", String.join("|", candidate)); // hash do vetor de CIDs

        return new IndexUpdate(
                "index_update",
                next,
                prev,
                cid,
                name,
                vectorDim,
                vector,
                cidsHash,
                System.currentTimeMillis() // ts em millis
        );
    }

    /** Commit definitivo da versão candidata. */
    public synchronized void commit(IndexUpdate upd) {
        if (upd.prev_version() != currentVersion.get()) {
            throw new IllegalStateException("commit com prev_version=" + upd.prev_version()
                    + " mas current=" + currentVersion.get());
        }
        committed.add(upd.cid());
        currentVersion.set(upd.version());
    }


    private static String sha256Hex(String prefix, String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] dig = md.digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(prefix == null ? "" : prefix);
            for (byte b : dig) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) { throw new RuntimeException(e); }
    }
    
    public static final class IndexUpdate {
        private final String kind;
        private final int version;
        private final int prev_version;
        private final String cid;
        private final String name;
        private final int vector_dim;
        private final float[] vector;
        private final String cids_hash;
        private final long ts;

        public IndexUpdate(String kind, int version, int prev_version, String cid, String name,
                           int vector_dim, float[] vector, String cids_hash, long ts) {
            this.kind = kind;
            this.version = version;
            this.prev_version = prev_version;
            this.cid = cid;
            this.name = name;
            this.vector_dim = vector_dim;
            this.vector = vector;
            this.cids_hash = cids_hash;
            this.ts = ts;
        }

        public String kind()        { return kind; }
        public int version()        { return version; }
        public int prev_version()   { return prev_version; }
        public String cid()         { return cid; }
        public String name()        { return name; }
        public int vector_dim()     { return vector_dim; }
        public float[] vector()     { return vector; }
        public String cids_hash()   { return cids_hash; }
        public long ts()            { return ts; }
    }
}