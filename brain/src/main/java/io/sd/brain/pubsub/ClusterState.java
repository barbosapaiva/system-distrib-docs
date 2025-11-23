package io.sd.brain.pubsub;
import org.springframework.stereotype.Component;

@Component
public class ClusterState {
    private static volatile long version = 0L;
    private static volatile String indexCid = null;

    public static void setVersionAndIndexCid(long v, String cid){ version = v; indexCid = cid; }
    public static long version(){ return version; }
    public static String indexCid(){ return indexCid; }
}