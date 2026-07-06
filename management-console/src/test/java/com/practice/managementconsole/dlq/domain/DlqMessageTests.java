package com.practice.managementconsole.dlq.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DlqMessageTests {

	@Test
	@DisplayName("신규_메시지_생성")
	void 신규_메시지_생성() {
		// given
		DlqMessage message = createMessage();

		// when
		DlqMessageStatus status = message.getStatus();

		// then
		assertThat(status).isEqualTo(DlqMessageStatus.NEW);
		assertThat(message.getSourceTopic()).isEqualTo("cdc-transform-events-local");
		assertThat(message.getSourcePartition()).isEqualTo(1);
		assertThat(message.getSourceOffset()).isEqualTo(10L);
	}

	@Test
	@DisplayName("재처리_상태변경")
	void 재처리_상태변경() {
		// given
		DlqMessage message = createMessage();

		// when
		message.markReplayed("cdc-loader-replay-local", "재처리 요청");

		// then
		assertThat(message.getStatus()).isEqualTo(DlqMessageStatus.REPLAYED);
		assertThat(message.getReplayTopic()).isEqualTo("cdc-loader-replay-local");
		assertThat(message.getMemo()).isEqualTo("재처리 요청");
		assertThat(message.getReplayedAt()).isNotNull();
	}

	@Test
	@DisplayName("무시_상태변경")
	void 무시_상태변경() {
		// given
		DlqMessage message = createMessage();

		// when
		message.ignore("운영 제외");

		// then
		assertThat(message.getStatus()).isEqualTo(DlqMessageStatus.IGNORED);
		assertThat(message.getMemo()).isEqualTo("운영 제외");
		assertThat(message.getIgnoredAt()).isNotNull();
	}

	@Test
	@DisplayName("완료_상태변경")
	void 완료_상태변경() {
		// given
		DlqMessage message = createMessage();

		// when
		message.complete("수동 보정 완료");

		// then
		assertThat(message.getStatus()).isEqualTo(DlqMessageStatus.COMPLETED);
		assertThat(message.getMemo()).isEqualTo("수동 보정 완료");
		assertThat(message.getCompletedAt()).isNotNull();
	}

	@Test
	@DisplayName("처리된메시지_중복처리_거부")
	void 처리된메시지_중복처리_거부() {
		// given
		DlqMessage message = createMessage();
		message.ignore("운영 제외");

		// when & then
		assertThatThrownBy(() -> message.markReplayed("cdc-loader-replay-local", "재처리 요청"))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("이미 처리된 DLQ 메시지입니다");
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
