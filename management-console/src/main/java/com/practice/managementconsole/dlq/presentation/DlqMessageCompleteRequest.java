package com.practice.managementconsole.dlq.presentation;

// DLQ 완료 처리 요청
public record DlqMessageCompleteRequest(
		String memo) {
}
