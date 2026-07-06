package com.practice.managementconsole.dlq.application;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.practice.managementconsole.dlq.domain.DlqMessage;
import com.practice.managementconsole.dlq.domain.DlqMessageRepository;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
// DLQ 메시지 조회 조건 조합 처리
public class DlqMessageQueryService {

	private final DlqMessageRepository dlqMessageRepository;

	// DLQ 메시지 목록 조회
	@Transactional(readOnly = true)
	public Page<DlqMessage> search(DlqMessageSearchCondition condition, Pageable pageable) {
		return dlqMessageRepository.findAll(specification(condition), pageable);
	}

	// DLQ 메시지 상세 조회
	@Transactional(readOnly = true)
	public DlqMessage get(Long id) {
		return dlqMessageRepository.findById(id)
				.orElseThrow(() -> new IllegalArgumentException("DLQ 메시지를 찾을 수 없습니다. id=" + id));
	}

	private Specification<DlqMessage> specification(DlqMessageSearchCondition condition) {
		return Specification.allOf(
				statusEquals(condition.status()),
				failureTypeEquals(condition.failureType()),
				stageEquals(condition.stage()),
				sourceTopicEquals(condition.sourceTopic()),
				createdAtGreaterThanOrEqual(condition.from()),
				createdAtLessThanOrEqual(condition.to()));
	}

	private Specification<DlqMessage> statusEquals(Object status) {
		return status == null ? null : (root, query, criteriaBuilder) -> criteriaBuilder.equal(root.get("status"), status);
	}

	private Specification<DlqMessage> failureTypeEquals(String failureType) {
		return hasNoText(failureType) ? null : (root, query, criteriaBuilder) ->
				criteriaBuilder.equal(root.get("failureType"), failureType);
	}

	private Specification<DlqMessage> stageEquals(String stage) {
		return hasNoText(stage) ? null : (root, query, criteriaBuilder) ->
				criteriaBuilder.equal(root.get("stage"), stage);
	}

	private Specification<DlqMessage> sourceTopicEquals(String sourceTopic) {
		return hasNoText(sourceTopic) ? null : (root, query, criteriaBuilder) ->
				criteriaBuilder.equal(root.get("sourceTopic"), sourceTopic);
	}

	private Specification<DlqMessage> createdAtGreaterThanOrEqual(Object from) {
		return from == null ? null : (root, query, criteriaBuilder) ->
				criteriaBuilder.greaterThanOrEqualTo(root.get("createdAt"), (java.time.LocalDateTime) from);
	}

	private Specification<DlqMessage> createdAtLessThanOrEqual(Object to) {
		return to == null ? null : (root, query, criteriaBuilder) ->
				criteriaBuilder.lessThanOrEqualTo(root.get("createdAt"), (java.time.LocalDateTime) to);
	}

	private boolean hasNoText(String value) {
		return value == null || value.isBlank();
	}

}
