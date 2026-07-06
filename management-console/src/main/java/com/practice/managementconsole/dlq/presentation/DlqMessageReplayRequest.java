package com.practice.managementconsole.dlq.presentation;

// DLQ replay 요청
public record DlqMessageReplayRequest(
		String replayTopic,
		String memo) {
}
