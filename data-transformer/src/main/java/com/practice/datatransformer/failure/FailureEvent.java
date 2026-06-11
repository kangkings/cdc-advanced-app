package com.practice.datatransformer.failure;

import java.time.Instant;
import java.util.Map;

public record FailureEvent(
		String stage,
		FailureType failureType,
		boolean retryable,
		String reason,
		String sourceTopic,
		Integer sourcePartition,
		Long sourceOffset,
		Instant failedAt,
		String originalMessage,
		Map<String, String> context) {

	public static FailureEvent of(
			FailureType failureType,
			boolean retryable,
			String reason,
			String sourceTopic,
			Integer sourcePartition,
			Long sourceOffset,
			String originalMessage,
			Map<String, String> context) {
		return new FailureEvent(
				"data-transformer",
				failureType,
				retryable,
				reason,
				sourceTopic,
				sourcePartition,
				sourceOffset,
				Instant.now(),
				originalMessage,
				context == null ? Map.of() : Map.copyOf(context));
	}

}
