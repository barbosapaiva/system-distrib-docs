package io.sd.brain.cluster;

import io.sd.brain.node.NodeRoleManager;
import io.sd.brain.pubsub.ClusterState;
import io.sd.brain.pubsub.PubSubService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class HeartbeatTask {

    private final NodeRoleManager roles;
    private final ClusterState cluster;
    private final PubSubService pub;
    private final String topic;

    public HeartbeatTask(NodeRoleManager roles,
                         ClusterState cluster,
                         PubSubService pub,
                         @Value("${pubsub.topic:sd-index}") String topic) {
        this.roles = roles;
        this.cluster = cluster;
        this.pub = pub;
        this.topic = topic;
    }

    @Scheduled(fixedDelayString = "${cluster.heartbeat.interval.ms:1500}")
    public void sendHeartbeat() {
        if (!roles.isLeader()) {
            return;
        }

        long now = System.currentTimeMillis();
        long term = roles.term();

        Map<String, Object> hb = new HashMap<>();
        hb.put("kind", "hb");
        hb.put("term", term);
        hb.put("id", roles.myId());
        hb.put("ts", now);

        pub.publishJson(topic, hb);
        cluster.setTermAndLeader(term, roles.myId());
    }
}