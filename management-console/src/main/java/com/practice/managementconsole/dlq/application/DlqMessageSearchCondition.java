package com.practice.managementconsole.dlq.application;

import java.time.LocalDateTime;

import com.practice.managementconsole.dlq.domain.DlqMessageStatus;

// DLQ 목록 조회 필터 조건
public record DlqMessageSearchCondition(
		DlqMessageStatus status,
		String failureType,
		String stage,
		String sourceTopic,
		LocalDateTime from,
		LocalDateTime to) {
}
