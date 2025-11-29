package io.sd.brain.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("node")
public record NodeProperties(String role, String outFile) { }