package com.practice.managementconsole.dlq.presentation;

import java.time.LocalDateTime;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.practice.managementconsole.dlq.application.DlqMessageQueryService;
import com.practice.managementconsole.dlq.application.DlqMessageReplayService;
import com.practice.managementconsole.dlq.application.DlqMessageSearchCondition;
import com.practice.managementconsole.dlq.application.DlqMessageStatusService;
import com.practice.managementconsole.dlq.domain.DlqMessageStatus;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/dlq/messages")
@RequiredArgsConstructor
// DLQ 메시지 운영 조회 API
public class DlqMessageController {

	private final DlqMessageQueryService dlqMessageQueryService;
	private final DlqMessageReplayService dlqMessageReplayService;
	private final DlqMessageStatusService dlqMessageStatusService;

	// DLQ 메시지 목록 조회
	@GetMapping
	public Page<DlqMessageSummaryResponse> search(
			@RequestParam(required = false) DlqMessageStatus status,
			@RequestParam(required = false) String failureType,
			@RequestParam(required = false) String stage,
			@RequestParam(required = false) String sourceTopic,
			@RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
			@RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to,
			Pageable pageable) {
		DlqMessageSearchCondition condition = new DlqMessageSearchCondition(
				status,
				failureType,
				stage,
				sourceTopic,
				from,
				to);
		return dlqMessageQueryService.search(condition, pageable)
				.map(DlqMessageSummaryResponse::from);
	}

	// DLQ 메시지 상세 조회
	@GetMapping("/{id}")
	public DlqMessageDetailResponse get(@PathVariable Long id) {
		return DlqMessageDetailResponse.from(dlqMessageQueryService.get(id));
	}

	// DLQ 메시지 replay topic 재발행
	@PostMapping("/{id}/replay")
	public DlqMessageDetailResponse replay(
			@PathVariable Long id,
			@RequestBody(required = false) DlqMessageReplayRequest request) {
		String replayTopic = request == null ? null : request.replayTopic();
		String memo = request == null ? null : request.memo();
		return DlqMessageDetailResponse.from(dlqMessageReplayService.replay(id, replayTopic, memo));
	}

	// DLQ 메시지 무시 처리
	@PostMapping("/{id}/ignore")
	public DlqMessageDetailResponse ignore(
			@PathVariable Long id,
			@RequestBody DlqMessageIgnoreRequest request) {
		return DlqMessageDetailResponse.from(dlqMessageStatusService.ignore(id, request.reason()));
	}

	// DLQ 메시지 완료 처리
	@PostMapping("/{id}/complete")
	public DlqMessageDetailResponse complete(
			@PathVariable Long id,
			@RequestBody DlqMessageCompleteRequest request) {
		return DlqMessageDetailResponse.from(dlqMessageStatusService.complete(id, request.memo()));
	}

}
