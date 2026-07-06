package com.practice.managementconsole.dlq.application;

import java.time.Instant;
import java.util.Map;

// DLQ topic 공통 실패 이벤트 역직렬화 모델
public record DlqFailureEvent(
		String stage,
		String failureType,
		boolean retryable,
		String reason,
		String sourceTopic,
		Integer sourcePartition,
		Long sourceOffset,
		Instant failedAt,
		String originalMessage,
		Map<String, String> context) {
}
