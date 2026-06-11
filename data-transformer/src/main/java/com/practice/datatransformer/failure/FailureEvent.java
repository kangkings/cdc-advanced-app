package com.practice.datatransformer.failure;

import java.time.Instant;
import java.util.Map;

// data-transformer DLQ로 발행할 공용 실패 이벤트
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

	// data-transformer stage를 고정한 실패 이벤트 생성
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
