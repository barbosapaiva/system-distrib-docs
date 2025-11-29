package io.sd.brain.config;

import io.sd.brain.node.ProcessorNode;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

public class WorkerRunnerConfig {

    @Bean
    @ConditionalOnProperty(name = "node.role", havingValue = "worker")
    ApplicationRunner startProcessor(ProcessorNode node) {
        return args -> {
            Thread t = new Thread(() -> {
                try {
                    node.run();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, "processor-node");
            t.setDaemon(false);
            t.start();
        };
    }
}