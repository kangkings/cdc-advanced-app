package com.practice.managementconsole.dlq.presentation;

// DLQ 무시 처리 요청
public record DlqMessageIgnoreRequest(
		String reason) {
}
