package com.practice.logscanner.logminer;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

// LogMiner 조회 대상 owner.table 설정 바인딩
@ConfigurationProperties(prefix = "logminer")
public record LogMinerTargetProperties(List<String> targets) {
}
