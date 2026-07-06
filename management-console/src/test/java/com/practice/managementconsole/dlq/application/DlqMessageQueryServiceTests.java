package com.practice.managementconsole.dlq.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;

import com.practice.managementconsole.dlq.domain.DlqMessage;
import com.practice.managementconsole.dlq.domain.DlqMessageRepository;

@ExtendWith(MockitoExtension.class)
class DlqMessageQueryServiceTests {

	@Mock
	private DlqMessageRepository dlqMessageRepository;

	@InjectMocks
	private DlqMessageQueryService dlqMessageQueryService;

	@Test
	@DisplayName("빈조건_목록조회")
	void 빈조건_목록조회() {
		// given
		Pageable pageable = PageRequest.of(0, 10);
		DlqMessageSearchCondition condition = new DlqMessageSearchCondition(null, null, null, null, null, null);
		DlqMessage message = createMessage();
		when(dlqMessageRepository.findAll(any(Specification.class), any(Pageable.class)))
				.thenReturn(new PageImpl<>(List.of(message), pageable, 1));

		// when
		Page<DlqMessage> result = dlqMessageQueryService.search(condition, pageable);

		// then
		assertThat(result.getContent()).containsExactly(message);
		verify(dlqMessageRepository).findAll(any(Specification.class), any(Pageable.class));
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
