package io.sd.brain.pubsub;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.net.UnknownHostException;
import java.util.Map;

@Component
@EnableScheduling
public class HeartbeatPublisher {
    private final PubSubService pub;
    private final String topic;
    private final String nodeId;

    public HeartbeatPublisher(
            PubSubService pub,
            @Value("${pubsub.topic:sd-index}") String topic
    ) {
        this.pub = pub;
        this.topic = topic;
        this.nodeId = resolveNodeId();
    }

    private String resolveNodeId() {
        try {
            String hn = java.net.InetAddress.getLocalHost().getHostName();
            if (hn != null && !hn.isBlank()) return hn;
        } catch (Exception ignored) {}
        String env = System.getenv("HOSTNAME");
        if (env != null && !env.isBlank()) return env;
        return "node-" + java.util.UUID.randomUUID();
    }

    @Scheduled(fixedRateString = "${hb.interval.ms:3000}")
    public void tick() {
        var hb = new java.util.HashMap<String,Object>();
        hb.put("kind", "hb");
        hb.put("role", "leader");
        hb.put("id", nodeId == null ? "unknown" : nodeId);
        hb.put("ts", System.currentTimeMillis());

        pub.publishJson(topic, hb);
    }
}