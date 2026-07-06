package com.practice.managementconsole.dlq.application;

import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import tools.jackson.databind.ObjectMapper;

@Component
@RequiredArgsConstructor
// DLQ raw JSON을 공통 실패 이벤트로 변환
public class DlqMessageParser {

	private final ObjectMapper objectMapper;

	// DLQ payload 역직렬화
	public DlqFailureEvent parse(String rawMessage) throws Exception {
		return objectMapper.readValue(rawMessage, DlqFailureEvent.class);
	}

}
