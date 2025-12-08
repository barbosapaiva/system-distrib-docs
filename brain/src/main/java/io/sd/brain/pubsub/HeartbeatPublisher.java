package io.sd.brain.pubsub;

import io.sd.brain.node.NodeRoleManager;
import io.sd.brain.index.VersionVectorService;
import org.springframework.scheduling.annotation.Scheduled;
import java.util.HashMap;
import java.util.Map;

import static java.lang.System.currentTimeMillis;

public class HeartbeatPublisher {
    private final PubSubService pub;
    private final ClusterState cluster;
    private final VersionVectorService vv;
    private final NodeRoleManager roles;
    private final String topic;

    public HeartbeatPublisher(PubSubService pub, ClusterState cluster, VersionVectorService vv,
                              NodeRoleManager roles, String topic) {
        this.pub = pub;
        this.cluster = cluster;
        this.vv = vv;
        this.roles = roles;
        this.topic = topic;
    }

//    @Scheduled(fixedRateString = "${hb.interval.ms:3000}")
//    public void tick() {
//        if (!roles.isLeader()) return;
//
//        Map<String, Object> hb = new HashMap<>();
//        hb.put("kind", "hb");
//        hb.put("term", roles.term());
//        hb.put("ts", currentTimeMillis());
//        hb.put("version", cluster.getVersion());
//        hb.put("index_cid", cluster.getIndexCid());
//
//        pub.publishJson(topic, hb);
//    }
}