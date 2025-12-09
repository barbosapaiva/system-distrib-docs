package io.sd.brain.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.sd.brain.node.NodeRoleManager;
import io.sd.brain.cluster.LeaderElectionTask;
import io.sd.brain.cluster.WorkerDirectory;
import io.sd.brain.consensus.AckService;
import io.sd.brain.node.RecoveryService;
import io.sd.brain.search.InMemoryIndex;
import io.sd.brain.node.ProcessorNode;
import io.sd.brain.pubsub.PubSubService;
import io.sd.brain.pubsub.PubSubSubscriber;
import io.sd.brain.pubsub.HeartbeatPublisher;
import io.sd.brain.pubsub.ClusterState;
import io.sd.brain.rest.DocumentController;
import io.sd.brain.emb.EmbeddingService;
import io.sd.brain.index.VersionVectorService;
import io.sd.brain.ipfs.IpfsClient;
import io.sd.brain.search.SearchJobRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Configuration
@EnableConfigurationProperties(NodeProperties.class)
public class NodeConfig {

    private static final Logger log = LoggerFactory.getLogger(NodeConfig.class);

    @Bean
    IpfsClient ipfsClient(@Value("${ipfs.api}") String api) {
        return new IpfsClient(api);
    }

    @Bean
    DocumentController documentController(
            ObjectMapper om,
            IpfsClient ipfs,
            EmbeddingService emb,
            VersionVectorService vv,
            PubSubService pubsub,
            AckService acks,
            ClusterState cluster,
            NodeRoleManager roles,
            @Value("${pubsub.topic:sd-index}") String topic,
            @Value("${cluster.quorum:1}") int quorum
    ) {
        return new DocumentController(om, ipfs, emb, vv, pubsub, acks, cluster, roles, topic, quorum);
    }

    @Bean
    public NodeRoleManager nodeRoleManager(
            @Value("${node.id:${HOSTNAME:unknown}}") String myId,
            @Value("${election.timeout.base.ms:5000}") long baseTimeout,
            @Value("${election.timeout.jitter.ms:3000}") long jitter
    ) {
        return new NodeRoleManager(myId, baseTimeout, jitter);
    }

    @Bean
    public HeartbeatPublisher heartbeatPublisher(NodeRoleManager roles,
                                                 ClusterState clusterState,
                                                 VersionVectorService vv,
                                                 PubSubService pubSub,
                                                 @Value("${pubsub.topic:sd-index}") String topic) {
        return new HeartbeatPublisher(pubSub, clusterState, vv, roles, topic);
    }

    @Bean
    PubSubSubscriber pubSubSubscriber(
            AckService acks,
            ClusterState cluster,
            NodeRoleManager roles,
            WorkerDirectory workers,
            SearchJobRegistry jobs,
            PubSubService pub,
            @Value("${ipfs.api}") String api,
            @Value("${pubsub.topic:sd-index}") String topic,
            @Value("${cluster.quorum:1}") int quorum
    ) {
        return new PubSubSubscriber(api, topic, acks, cluster, roles, workers, jobs, pub, quorum);
    }

    @Bean
    ProcessorNode processorNode(
            InMemoryIndex inMemoryIndex,
            RecoveryService recoveryService,
            @Value("${ipfs.api}") String api,
            @Value("${pubsub.topic:sd-index}") String topic,
            NodeProperties props
    ) throws Exception {
        return new ProcessorNode(api, topic, props.outFile(), recoveryService, inMemoryIndex);
    }

    @Bean
    ApplicationRunner startProcessor(ProcessorNode node) {
        return args -> {
            Thread t = new Thread(() -> {
                try { node.run(); } catch (Exception e) { e.printStackTrace(); }
            }, "processor-loop");
            t.setDaemon(true);
            t.start();
        };
    }

    @Bean
    ApplicationRunner logRole(@Value("${node.role:leader}") String role,
                              @Value("${server.port:disabled}") String port) {
        return args -> log.info("Node role={}, server.port={}", role, port);
    }

    @Bean
    public WorkerDirectory workerDirectory() { return new WorkerDirectory(); }

    @Bean
    public LeaderElectionTask leaderElectionTask(NodeRoleManager roles,
                                                 ClusterState clusterState,
                                                 PubSubService pubSub,
                                                 @Value("${pubsub.topic:sd-index}") String topic,
                                                 @Value("${cluster.quorum:1}") int quorum) {
        return new LeaderElectionTask(roles, clusterState, pubSub, topic, quorum);
    }
}