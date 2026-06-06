package com.practice.logscanner.logminer;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "logminer")
public record LogMinerTargetProperties(List<String> targets) {
}
