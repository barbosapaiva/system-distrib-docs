package io.sd.brain.cluster;

import io.sd.brain.pubsub.ClusterState;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@EnableScheduling
public class LeaderElectionTask {

    private final NodeRoleManager roles;
    private final ClusterState cluster;
    private final long timeoutMs;

    public LeaderElectionTask(
            NodeRoleManager roles,
            ClusterState cluster,
            @Value("${leader.timeout.ms:10000}") long timeoutMs
    ) {
        this.roles = roles;
        this.cluster = cluster;
        this.timeoutMs = timeoutMs;
    }

    @Scheduled(fixedDelayString = "${election.tick.ms:1500}")
    public void tick() {
        long last = roles.lastHeartbeatTs();
        long now = System.currentTimeMillis();

        if (roles.isLeader()) return;

        if (last == 0 || (now - last) > timeoutMs) {
            boolean promoted = roles.tryPromote();
            if (promoted) {
                cluster.setTermAndLeader(roles.term(), roles.myId());
            }
        }
    }
}