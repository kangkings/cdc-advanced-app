package com.practice.managementconsole.dlq.application;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import com.practice.managementconsole.dlq.config.DlqTopicProperties;
import com.practice.managementconsole.dlq.domain.DlqMessage;
import com.practice.managementconsole.dlq.domain.DlqMessageRepository;
import com.practice.managementconsole.dlq.domain.DlqMessageStatus;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static org.springframework.http.HttpStatus.CONFLICT;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.UNPROCESSABLE_ENTITY;

@Slf4j
@Service
@RequiredArgsConstructor
// DLQ 메시지 replay topic 재발행 처리
public class DlqMessageReplayService {

	private final DlqMessageRepository dlqMessageRepository;
	private final KafkaTemplate<String, String> kafkaTemplate;
	private final DlqTopicProperties dlqTopicProperties;

	// replay topic 발행 후 상태 변경
	@Transactional
	public DlqMessage replay(Long id, String replayTopic, String memo) {
		DlqMessage message = dlqMessageRepository.findById(id)
				.orElseThrow(() -> new ResponseStatusException(NOT_FOUND, "DLQ 메시지를 찾을 수 없습니다. id=" + id));
		validateReplayable(message);

		String targetTopic = resolveReplayTopic(replayTopic);
		String key = createReplayKey(message);
		kafkaTemplate.send(targetTopic, key, message.getOriginalMessage()).join();
		markReplayed(message, targetTopic, memo);

		log.info("[DLQ-MESSAGE][REPLAYED] id={}, topic={}, key={}", id, targetTopic, key);
		return message;
	}

	private void validateReplayable(DlqMessage message) {
		if (message.getStatus() != DlqMessageStatus.NEW) {
			throw new ResponseStatusException(CONFLICT, "이미 처리된 DLQ 메시지입니다. status=" + message.getStatus());
		}
		if (message.getOriginalMessage() == null || message.getOriginalMessage().isBlank()) {
			throw new ResponseStatusException(UNPROCESSABLE_ENTITY, "재처리할 originalMessage가 없습니다.");
		}
	}

	private void markReplayed(DlqMessage message, String replayTopic, String memo) {
		try {
			message.markReplayed(replayTopic, memo);
		}
		catch (IllegalStateException ex) {
			throw new ResponseStatusException(CONFLICT, ex.getMessage(), ex);
		}
	}

	private String resolveReplayTopic(String replayTopic) {
		if (replayTopic != null && !replayTopic.isBlank()) {
			return replayTopic;
		}
		return dlqTopicProperties.replayTopic();
	}

	private String createReplayKey(DlqMessage message) {
		return "%s:%s:%s".formatted(
				message.getSourceTopic(),
				message.getSourcePartition(),
				message.getSourceOffset());
	}

}
