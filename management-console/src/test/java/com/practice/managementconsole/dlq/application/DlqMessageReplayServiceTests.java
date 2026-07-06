package com.practice.managementconsole.dlq.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.server.ResponseStatusException;

import com.practice.managementconsole.dlq.config.DlqTopicProperties;
import com.practice.managementconsole.dlq.domain.DlqMessage;
import com.practice.managementconsole.dlq.domain.DlqMessageRepository;
import com.practice.managementconsole.dlq.domain.DlqMessageStatus;

@ExtendWith(MockitoExtension.class)
class DlqMessageReplayServiceTests {

	@Mock
	private DlqMessageRepository dlqMessageRepository;

	@Mock
	private KafkaTemplate<String, String> kafkaTemplate;

	@Mock
	private DlqTopicProperties dlqTopicProperties;

	@InjectMocks
	private DlqMessageReplayService dlqMessageReplayService;

	@Test
	@DisplayName("재처리_발행성공")
	void 재처리_발행성공() {
		// given
		DlqMessage message = createMessage();
		when(dlqMessageRepository.findById(1L)).thenReturn(Optional.of(message));
		when(kafkaTemplate.send("cdc-loader-replay-local", "cdc-transform-events-local:1:10", "{\"payload\":true}"))
				.thenReturn(CompletableFuture.completedFuture(null));

		// when
		DlqMessage result = dlqMessageReplayService.replay(1L, "cdc-loader-replay-local", "재처리 요청");

		// then
		assertThat(result.getStatus()).isEqualTo(DlqMessageStatus.REPLAYED);
		assertThat(result.getReplayTopic()).isEqualTo("cdc-loader-replay-local");
		assertThat(result.getMemo()).isEqualTo("재처리 요청");
	}

	@Test
	@DisplayName("기본토픽_발행")
	void 기본토픽_발행() {
		// given
		DlqMessage message = createMessage();
		when(dlqMessageRepository.findById(1L)).thenReturn(Optional.of(message));
		when(dlqTopicProperties.replayTopic()).thenReturn("cdc-loader-replay-local");
		when(kafkaTemplate.send("cdc-loader-replay-local", "cdc-transform-events-local:1:10", "{\"payload\":true}"))
				.thenReturn(CompletableFuture.completedFuture(null));

		// when
		DlqMessage result = dlqMessageReplayService.replay(1L, null, "재처리 요청");

		// then
		assertThat(result.getReplayTopic()).isEqualTo("cdc-loader-replay-local");
	}

	@Test
	@DisplayName("처리완료_재처리거부")
	void 처리완료_재처리거부() {
		// given
		DlqMessage message = createMessage();
		message.ignore("운영 제외");
		when(dlqMessageRepository.findById(1L)).thenReturn(Optional.of(message));

		// when & then
		assertThatThrownBy(() -> dlqMessageReplayService.replay(1L, "cdc-loader-replay-local", "재처리 요청"))
				.isInstanceOf(ResponseStatusException.class)
				.hasMessageContaining("409 CONFLICT");
		verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
	}

	@Test
	@DisplayName("원본문자열없음_재처리거부")
	void 원본문자열없음_재처리거부() {
		// given
		DlqMessage message = DlqMessage.create(
				"management-console",
				"DLQ_PARSE_FAILED",
				"파싱 실패",
				false,
				"cdc-loader-dlq-local",
				0,
				1L,
				null,
				"{invalid");
		when(dlqMessageRepository.findById(1L)).thenReturn(Optional.of(message));

		// when & then
		assertThatThrownBy(() -> dlqMessageReplayService.replay(1L, "cdc-loader-replay-local", "재처리 요청"))
				.isInstanceOf(ResponseStatusException.class)
				.hasMessageContaining("422 UNPROCESSABLE_ENTITY");
		verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
	}

	private DlqMessage createMessage() {
		return DlqMessage.create(
				"data-loader",
				"MISSING_REQUIRED_FIELD",
				"필수 컬럼 없음",
				false,
				"cdc-transform-events-local",
				1,
				10L,
				"{\"payload\":true}",
				"{\"stage\":\"data-loader\"}");
	}

}
