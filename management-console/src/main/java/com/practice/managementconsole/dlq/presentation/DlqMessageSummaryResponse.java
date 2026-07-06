package com.practice.managementconsole.dlq.presentation;

import java.time.LocalDateTime;

import com.practice.managementconsole.dlq.domain.DlqMessage;
import com.practice.managementconsole.dlq.domain.DlqMessageStatus;

// DLQ 목록 조회 응답
public record DlqMessageSummaryResponse(
		Long id,
		String stage,
		String failureType,
		String reason,
		boolean retryable,
		String sourceTopic,
		Integer sourcePartition,
		Long sourceOffset,
		DlqMessageStatus status,
		LocalDateTime createdAt,
		LocalDateTime updatedAt) {

	// entity를 목록 응답으로 변환
	public static DlqMessageSummaryResponse from(DlqMessage message) {
		return new DlqMessageSummaryResponse(
				message.getId(),
				message.getStage(),
				message.getFailureType(),
				message.getReason(),
				message.isRetryable(),
				message.getSourceTopic(),
				message.getSourcePartition(),
				message.getSourceOffset(),
				message.getStatus(),
				message.getCreatedAt(),
				message.getUpdatedAt());
	}

}
