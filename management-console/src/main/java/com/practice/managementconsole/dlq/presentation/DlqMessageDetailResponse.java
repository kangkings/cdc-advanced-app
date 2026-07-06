package com.practice.managementconsole.dlq.presentation;

import java.time.LocalDateTime;

import com.practice.managementconsole.dlq.domain.DlqMessage;
import com.practice.managementconsole.dlq.domain.DlqMessageStatus;

// DLQ 상세 조회 응답
public record DlqMessageDetailResponse(
		Long id,
		String stage,
		String failureType,
		String reason,
		boolean retryable,
		String sourceTopic,
		Integer sourcePartition,
		Long sourceOffset,
		String originalMessage,
		String rawDlqMessage,
		DlqMessageStatus status,
		String replayTopic,
		String memo,
		LocalDateTime createdAt,
		LocalDateTime updatedAt,
		LocalDateTime replayedAt,
		LocalDateTime ignoredAt,
		LocalDateTime completedAt) {

	// entity를 상세 응답으로 변환
	public static DlqMessageDetailResponse from(DlqMessage message) {
		return new DlqMessageDetailResponse(
				message.getId(),
				message.getStage(),
				message.getFailureType(),
				message.getReason(),
				message.isRetryable(),
				message.getSourceTopic(),
				message.getSourcePartition(),
				message.getSourceOffset(),
				message.getOriginalMessage(),
				message.getRawDlqMessage(),
				message.getStatus(),
				message.getReplayTopic(),
				message.getMemo(),
				message.getCreatedAt(),
				message.getUpdatedAt(),
				message.getReplayedAt(),
				message.getIgnoredAt(),
				message.getCompletedAt());
	}

}
