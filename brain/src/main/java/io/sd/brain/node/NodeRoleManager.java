package io.sd.brain.node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

public class NodeRoleManager {

    private static final Logger log = LoggerFactory.getLogger(NodeRoleManager.class);

    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    private final String myId;
    private final long baseTimeoutMs;
    private final long jitterMs;

    private Role role = Role.FOLLOWER;
    private long term = 0;
    private String leaderId = null;

    private long lastHeartbeatTs;
    private long currentTimeoutMs;

    private String votedFor = null;
    private int votesGranted = 0;
    private long electionStartTs = 0L;

    public NodeRoleManager(String myId, long baseTimeoutMs, long jitterMs) {
        this.myId = myId;
        this.baseTimeoutMs = baseTimeoutMs;
        this.jitterMs = jitterMs;

        long now = System.currentTimeMillis();
        this.lastHeartbeatTs = now;
        resetTimeout(now);

        log.info("NodeRoleManager inicializado para {} com timeout base={}ms jitter={}ms",
                myId, baseTimeoutMs, jitterMs);
    }


    private long computeTimeoutMs() {
        if (jitterMs <= 0) return baseTimeoutMs;
        long extra = ThreadLocalRandom.current().nextLong(jitterMs);
        return baseTimeoutMs + extra;
    }

    private void resetTimeout(long now) {
        this.currentTimeoutMs = computeTimeoutMs();
        this.lastHeartbeatTs = now;
    }

    private void resetElectionState() {
        this.votedFor = null;
        this.votesGranted = 0;
        this.electionStartTs = 0L;
    }


    public synchronized boolean isLeader() { return role == Role.LEADER; }
    public synchronized long term() { return term; }
    public synchronized String leaderId() { return leaderId; }
    public synchronized String myId() { return myId; }


    public synchronized void onHeartbeat(long hbTerm, String hbLeaderId, long hbTs) {
        if (hbTerm < this.term) {
            return;
        }

        long now = System.currentTimeMillis();

        if (hbTerm > this.term) {
            this.term = hbTerm;
            this.leaderId = hbLeaderId;
            this.role = Role.FOLLOWER;
            resetElectionState();
            log.info("Aceitei líder {} no term {} (via heartbeat)", hbLeaderId, hbTerm);
        } else {
            if (leaderId == null || !hbLeaderId.equals(this.leaderId)) {
                this.leaderId = hbLeaderId;
                log.info("Atualizei leaderId={} no term {}", hbLeaderId, hbTerm);
            }
            this.role = Role.FOLLOWER;
        }

        resetTimeout(now);
    }

    public synchronized boolean onElectionTick(long now) {

        if (role == Role.LEADER) {
            return false;
        }

        long elapsed = now - lastHeartbeatTs;
        if (elapsed < currentTimeoutMs) {
            return false;
        }

        this.term = this.term + 1;
        this.role = Role.CANDIDATE;
        this.leaderId = null;
        this.votedFor = myId;       // voto em mim próprio
        this.votesGranted = 1;      // auto-voto
        this.electionStartTs = now;
        resetTimeout(now);

        log.info("Timeout de eleição expirou. Nó {} torna-se CANDIDATE no term {} (elapsed={}ms, timeout={}ms).",
                myId, term, elapsed, currentTimeoutMs);

        return true;
    }


    public static class VoteDecision {
        public final boolean granted;
        public final long term;

        public VoteDecision(boolean granted, long term) {
            this.granted = granted;
            this.term = term;
        }
    }

    public synchronized VoteDecision onVoteRequest(long candidateTerm, String candidateId, long now) {

        if (candidateTerm < this.term) {
            return new VoteDecision(false, this.term);
        }

        if (candidateTerm > this.term) {
            this.term = candidateTerm;
            this.role = Role.FOLLOWER;
            this.leaderId = null;
            resetElectionState();
            log.info("Atualizei term para {} por causa de pedido de voto de {}", candidateTerm, candidateId);
        }

        boolean canVote = (votedFor == null || votedFor.equals(candidateId));

        if (canVote) {
            this.votedFor = candidateId;
            this.role = Role.FOLLOWER;
            resetTimeout(now);
            log.info("Votei em {} no term {}", candidateId, this.term);
            return new VoteDecision(true, this.term);
        } else {
            log.info("Rejeitei voto para {} no term {} (já tinha votado em {}).", candidateId, this.term, votedFor);
            return new VoteDecision(false, this.term);
        }
    }


    public synchronized boolean onVoteResponse(long respTerm,
                                               boolean voteGranted,
                                               String voterId,
                                               int quorum,
                                               long now) {
        if (respTerm > this.term) {
            log.info("Resposta de voto com term mais recente ({} > {}). Step-down para FOLLOWER.", respTerm, this.term);
            this.term = respTerm;
            this.role = Role.FOLLOWER;
            this.leaderId = null;
            resetElectionState();
            resetTimeout(now);
            return false;
        }

        if (respTerm < this.term) {
            return false;
        }

        if (this.role != Role.CANDIDATE) {
            return false;
        }

        if (!voteGranted) {
            log.debug("Voto NEGADO por {} no term {}.", voterId, respTerm);
            return false;
        }

        this.votesGranted += 1;
        log.info("Voto CONCEDIDO por {} no term {}. Total de votos={}.", voterId, respTerm, votesGranted);

        if (this.votesGranted >= quorum) {
            this.role = Role.LEADER;
            this.leaderId = myId;
            resetTimeout(now);
            log.warn("Nó {} TORNOU-SE LEADER no term {} com {} votos (quorum={})",
                    myId, term, votesGranted, quorum);
            return true;
        }

        return false;
    }
}