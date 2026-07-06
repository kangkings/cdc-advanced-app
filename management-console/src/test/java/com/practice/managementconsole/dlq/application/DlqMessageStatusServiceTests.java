package com.practice.managementconsole.dlq.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.server.ResponseStatusException;

import com.practice.managementconsole.dlq.domain.DlqMessage;
import com.practice.managementconsole.dlq.domain.DlqMessageRepository;
import com.practice.managementconsole.dlq.domain.DlqMessageStatus;

@ExtendWith(MockitoExtension.class)
class DlqMessageStatusServiceTests {

	@Mock
	private DlqMessageRepository dlqMessageRepository;

	@InjectMocks
	private DlqMessageStatusService dlqMessageStatusService;

	@Test
	@DisplayName("무시_처리")
	void 무시_처리() {
		// given
		DlqMessage message = createMessage();
		when(dlqMessageRepository.findById(1L)).thenReturn(Optional.of(message));

		// when
		DlqMessage result = dlqMessageStatusService.ignore(1L, "운영 제외");

		// then
		assertThat(result.getStatus()).isEqualTo(DlqMessageStatus.IGNORED);
		assertThat(result.getMemo()).isEqualTo("운영 제외");
	}

	@Test
	@DisplayName("완료_처리")
	void 완료_처리() {
		// given
		DlqMessage message = createMessage();
		when(dlqMessageRepository.findById(1L)).thenReturn(Optional.of(message));

		// when
		DlqMessage result = dlqMessageStatusService.complete(1L, "수동 보정 완료");

		// then
		assertThat(result.getStatus()).isEqualTo(DlqMessageStatus.COMPLETED);
		assertThat(result.getMemo()).isEqualTo("수동 보정 완료");
	}

	@Test
	@DisplayName("처리완료_무시거부")
	void 처리완료_무시거부() {
		// given
		DlqMessage message = createMessage();
		message.complete("수동 보정 완료");
		when(dlqMessageRepository.findById(1L)).thenReturn(Optional.of(message));

		// when & then
		assertThatThrownBy(() -> dlqMessageStatusService.ignore(1L, "운영 제외"))
				.isInstanceOf(ResponseStatusException.class)
				.hasMessageContaining("409 CONFLICT");
	}

	@Test
	@DisplayName("없는메시지_완료거부")
	void 없는메시지_완료거부() {
		// given
		when(dlqMessageRepository.findById(1L)).thenReturn(Optional.empty());

		// when & then
		assertThatThrownBy(() -> dlqMessageStatusService.complete(1L, "수동 보정 완료"))
				.isInstanceOf(ResponseStatusException.class)
				.hasMessageContaining("404 NOT_FOUND");
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
