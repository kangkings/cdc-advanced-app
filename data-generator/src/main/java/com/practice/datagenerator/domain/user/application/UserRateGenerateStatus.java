package com.practice.datagenerator.domain.user.application;

import java.time.LocalDateTime;

public record UserRateGenerateStatus(
		boolean running,
		String status,
		int rate,
		int durationSeconds,
		long expectedTotal,
		long generatedCount,
		long failedCount,
		LocalDateTime startedAt,
		LocalDateTime endedAt,
		long elapsedSeconds,
		String lastError
) {
}
