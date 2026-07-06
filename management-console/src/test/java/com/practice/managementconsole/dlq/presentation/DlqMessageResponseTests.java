package com.practice.managementconsole.dlq.presentation;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.practice.managementconsole.dlq.domain.DlqMessage;
import com.practice.managementconsole.dlq.domain.DlqMessageStatus;

class DlqMessageResponseTests {

	@Test
	@DisplayName("목록응답_변환")
	void 목록응답_변환() {
		// given
		DlqMessage message = createMessage();

		// when
		DlqMessageSummaryResponse response = DlqMessageSummaryResponse.from(message);

		// then
		assertThat(response.stage()).isEqualTo("data-loader");
		assertThat(response.failureType()).isEqualTo("MISSING_REQUIRED_FIELD");
		assertThat(response.status()).isEqualTo(DlqMessageStatus.NEW);
		assertThat(response.sourceTopic()).isEqualTo("cdc-transform-events-local");
	}

	@Test
	@DisplayName("상세응답_변환")
	void 상세응답_변환() {
		// given
		DlqMessage message = createMessage();

		// when
		DlqMessageDetailResponse response = DlqMessageDetailResponse.from(message);

		// then
		assertThat(response.originalMessage()).isEqualTo("{\"payload\":true}");
		assertThat(response.rawDlqMessage()).isEqualTo("{\"stage\":\"data-loader\"}");
		assertThat(response.retryable()).isFalse();
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
