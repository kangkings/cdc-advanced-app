package com.practice.dataloader.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

// Kafka listener bounded retry 설정 바인딩
@ConfigurationProperties(prefix = "cdc.retry")
public record RetryProperties(
		int maxAttempts,
		long initialIntervalMs,
		double multiplier,
		long maxIntervalMs
) {

	// 비정상 설정값을 최소 안전값으로 보정
	public RetryProperties {
		if (maxAttempts < 1) {
			maxAttempts = 1;
		}
		if (initialIntervalMs < 1) {
			initialIntervalMs = 1000L;
		}
		if (multiplier < 1.0) {
			multiplier = 1.0;
		}
		if (maxIntervalMs < initialIntervalMs) {
			maxIntervalMs = initialIntervalMs;
		}
	}

}
