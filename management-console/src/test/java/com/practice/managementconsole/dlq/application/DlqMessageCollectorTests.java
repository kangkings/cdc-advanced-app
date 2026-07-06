package com.practice.managementconsole.dlq.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.practice.managementconsole.dlq.domain.DlqMessage;
import com.practice.managementconsole.dlq.domain.DlqMessageRepository;

@ExtendWith(MockitoExtension.class)
class DlqMessageCollectorTests {

	@Mock
	private DlqMessageRepository dlqMessageRepository;

	@Mock
	private DlqMessageParser dlqMessageParser;

	@InjectMocks
	private DlqMessageCollector dlqMessageCollector;

	@Test
	@DisplayName("신규DLQ_저장")
	void 신규DLQ_저장() throws Exception {
		// given
		String rawMessage = "{\"stage\":\"data-loader\"}";
		DlqFailureEvent event = failureEvent();
		when(dlqMessageParser.parse(rawMessage)).thenReturn(event);
		when(dlqMessageRepository.findBySourceTopicAndSourcePartitionAndSourceOffsetAndStage(
				"cdc-transform-events-local",
				1,
				10L,
				"data-loader"))
				.thenReturn(Optional.empty());
		when(dlqMessageRepository.save(any(DlqMessage.class)))
				.thenAnswer(invocation -> invocation.getArgument(0));

		// when
		DlqMessage saved = dlqMessageCollector.collect(rawMessage, "cdc-loader-dlq-local", 0, 5L);

		// then
		ArgumentCaptor<DlqMessage> captor = ArgumentCaptor.forClass(DlqMessage.class);
		verify(dlqMessageRepository).save(captor.capture());
		assertThat(saved.getStage()).isEqualTo("data-loader");
		assertThat(captor.getValue().getRawDlqMessage()).isEqualTo(rawMessage);
	}

	@Test
	@DisplayName("중복DLQ_저장생략")
	void 중복DLQ_저장생략() throws Exception {
		// given
		String rawMessage = "{\"stage\":\"data-loader\"}";
		DlqFailureEvent event = failureEvent();
		DlqMessage existing = DlqMessage.create(
				"data-loader",
				"MISSING_REQUIRED_FIELD",
				"필수 컬럼 없음",
				false,
				"cdc-transform-events-local",
				1,
				10L,
				"{\"payload\":true}",
				rawMessage);
		when(dlqMessageParser.parse(rawMessage)).thenReturn(event);
		when(dlqMessageRepository.findBySourceTopicAndSourcePartitionAndSourceOffsetAndStage(
				"cdc-transform-events-local",
				1,
				10L,
				"data-loader"))
				.thenReturn(Optional.of(existing));

		// when
		DlqMessage result = dlqMessageCollector.collect(rawMessage, "cdc-loader-dlq-local", 0, 5L);

		// then
		assertThat(result).isSameAs(existing);
		verify(dlqMessageRepository, never()).save(any());
	}

	@Test
	@DisplayName("파싱실패_원본DLQ위치저장")
	void 파싱실패_원본DLQ위치저장() throws Exception {
		// given
		String rawMessage = "{invalid";
		when(dlqMessageParser.parse(rawMessage)).thenThrow(new IllegalArgumentException("invalid json"));
		when(dlqMessageRepository.findBySourceTopicAndSourcePartitionAndSourceOffsetAndStage(
				"cdc-loader-dlq-local",
				0,
				5L,
				"management-console"))
				.thenReturn(Optional.empty());
		when(dlqMessageRepository.save(any(DlqMessage.class)))
				.thenAnswer(invocation -> invocation.getArgument(0));

		// when
		DlqMessage saved = dlqMessageCollector.collect(rawMessage, "cdc-loader-dlq-local", 0, 5L);

		// then
		assertThat(saved.getStage()).isEqualTo("management-console");
		assertThat(saved.getFailureType()).isEqualTo("DLQ_PARSE_FAILED");
		assertThat(saved.getSourceTopic()).isEqualTo("cdc-loader-dlq-local");
		assertThat(saved.getSourcePartition()).isZero();
		assertThat(saved.getSourceOffset()).isEqualTo(5L);
	}

	private DlqFailureEvent failureEvent() {
		return new DlqFailureEvent(
				"data-loader",
				"MISSING_REQUIRED_FIELD",
				false,
				"필수 컬럼 없음",
				"cdc-transform-events-local",
				1,
				10L,
				null,
				"{\"payload\":true}",
				null);
	}

}
