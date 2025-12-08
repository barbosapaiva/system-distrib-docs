package io.sd.brain.cluster;

import io.sd.brain.node.NodeRoleManager;
import io.sd.brain.pubsub.ClusterState;
import io.sd.brain.pubsub.PubSubService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@EnableScheduling
public class LeaderElectionTask {

    private static final Logger log = LoggerFactory.getLogger(LeaderElectionTask.class);

    private final NodeRoleManager roles;
    private final ClusterState cluster;
    private final PubSubService pub;
    private final String topic;
    private final int quorum;  // maioria, ex: 2 num cluster de 3

    public LeaderElectionTask(NodeRoleManager roles,
                              ClusterState cluster,
                              PubSubService pub,
                              @Value("${pubsub.topic:sd-index}") String topic,
                              @Value("${cluster.quorum:1}") int quorum) {
        this.roles = roles;
        this.cluster = cluster;
        this.pub = pub;
        this.topic = topic;
        this.quorum = quorum;
    }

    @Scheduled(fixedDelayString = "${election.check.interval.ms:250}")
    public void tick() {
        long now = System.currentTimeMillis();

        boolean startedElection = roles.onElectionTick(now);
        if (!startedElection) {
            return;
        }

        long term = roles.term();
        String candidateId = roles.myId();

        Map<String, Object> msg = new HashMap<>();
        msg.put("kind", "vote_req");
        msg.put("term", term);
        msg.put("candidate_id", candidateId);
        msg.put("ts", now);

        pub.publishJson(topic, msg);
        log.warn("Eleiçao iniciada por {} no term {}. Pedido de voto difundido no tópico {} (quorum={})",
                candidateId, term, topic, quorum);

    }
}