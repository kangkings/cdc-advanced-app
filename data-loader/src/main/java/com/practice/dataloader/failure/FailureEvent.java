package com.practice.dataloader.failure;

import java.time.Instant;
import java.util.Map;

// data-loader DLQ로 발행할 공용 실패 이벤트
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
		Map<String, String> context
) {

	// data-loader 실패 이벤트 공통 포맷 생성
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
				"data-loader",
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
