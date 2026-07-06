package com.practice.managementconsole.dlq.application;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.practice.managementconsole.dlq.domain.DlqMessage;
import com.practice.managementconsole.dlq.domain.DlqMessageRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
// DLQ 메시지 파싱과 Oracle 저장 처리
public class DlqMessageCollector {

	private static final String PARSE_FAILED_STAGE = "management-console";
	private static final String PARSE_FAILED_TYPE = "DLQ_PARSE_FAILED";

	private final DlqMessageRepository dlqMessageRepository;
	private final DlqMessageParser dlqMessageParser;

	// DLQ 메시지를 중복 없이 저장
	@Transactional
	public DlqMessage collect(String rawMessage, String dlqTopic, int dlqPartition, long dlqOffset) {
		DlqMessage message = toMessage(rawMessage, dlqTopic, dlqPartition, dlqOffset);
		return dlqMessageRepository.findBySourceTopicAndSourcePartitionAndSourceOffsetAndStage(
						message.getSourceTopic(),
						message.getSourcePartition(),
						message.getSourceOffset(),
						message.getStage())
				.orElseGet(() -> save(message));
	}

	private DlqMessage save(DlqMessage message) {
		DlqMessage saved = dlqMessageRepository.save(message);
		log.info("[DLQ-MESSAGE][SAVED] stage={}, failureType={}, sourceTopic={}, sourcePartition={}, sourceOffset={}",
				saved.getStage(),
				saved.getFailureType(),
				saved.getSourceTopic(),
				saved.getSourcePartition(),
				saved.getSourceOffset());
		return saved;
	}

	private DlqMessage toMessage(String rawMessage, String dlqTopic, int dlqPartition, long dlqOffset) {
		try {
			DlqFailureEvent event = dlqMessageParser.parse(rawMessage);
			return DlqMessage.create(
					event.stage(),
					event.failureType(),
					event.reason(),
					event.retryable(),
					event.sourceTopic(),
					event.sourcePartition(),
					event.sourceOffset(),
					event.originalMessage(),
					rawMessage);
		}
		catch (Exception ex) {
			log.warn("[DLQ-MESSAGE][PARSE-FAILED] topic={}, partition={}, offset={}, error={}",
					dlqTopic,
					dlqPartition,
					dlqOffset,
					ex.getMessage());
			return DlqMessage.create(
					PARSE_FAILED_STAGE,
					PARSE_FAILED_TYPE,
					"DLQ 메시지 파싱 실패: " + ex.getMessage(),
					false,
					dlqTopic,
					dlqPartition,
					dlqOffset,
					null,
					rawMessage);
		}
	}

}
