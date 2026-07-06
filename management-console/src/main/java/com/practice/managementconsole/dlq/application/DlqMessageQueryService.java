package com.practice.managementconsole.dlq.application;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

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
		return (root, query, criteriaBuilder) -> {
			List<Predicate> predicates = new ArrayList<>();
			statusEquals(condition, root, criteriaBuilder).ifPresent(predicates::add);
			failureTypeEquals(condition, root, criteriaBuilder).ifPresent(predicates::add);
			stageEquals(condition, root, criteriaBuilder).ifPresent(predicates::add);
			sourceTopicEquals(condition, root, criteriaBuilder).ifPresent(predicates::add);
			createdAtGreaterThanOrEqual(condition, root, criteriaBuilder).ifPresent(predicates::add);
			createdAtLessThanOrEqual(condition, root, criteriaBuilder).ifPresent(predicates::add);
			return criteriaBuilder.and(predicates.toArray(Predicate[]::new));
		};
	}

	private Optional<Predicate> statusEquals(
			DlqMessageSearchCondition condition,
			Root<DlqMessage> root,
			CriteriaBuilder criteriaBuilder) {
		return Optional.ofNullable(condition.status())
				.map(status -> criteriaBuilder.equal(root.get("status"), status));
	}

	private Optional<Predicate> failureTypeEquals(
			DlqMessageSearchCondition condition,
			Root<DlqMessage> root,
			CriteriaBuilder criteriaBuilder) {
		return hasNoText(condition.failureType())
				? Optional.empty()
				: Optional.of(criteriaBuilder.equal(root.get("failureType"), condition.failureType()));
	}

	private Optional<Predicate> stageEquals(
			DlqMessageSearchCondition condition,
			Root<DlqMessage> root,
			CriteriaBuilder criteriaBuilder) {
		return hasNoText(condition.stage())
				? Optional.empty()
				: Optional.of(criteriaBuilder.equal(root.get("stage"), condition.stage()));
	}

	private Optional<Predicate> sourceTopicEquals(
			DlqMessageSearchCondition condition,
			Root<DlqMessage> root,
			CriteriaBuilder criteriaBuilder) {
		return hasNoText(condition.sourceTopic())
				? Optional.empty()
				: Optional.of(criteriaBuilder.equal(root.get("sourceTopic"), condition.sourceTopic()));
	}

	private Optional<Predicate> createdAtGreaterThanOrEqual(
			DlqMessageSearchCondition condition,
			Root<DlqMessage> root,
			CriteriaBuilder criteriaBuilder) {
		return Optional.ofNullable(condition.from())
				.map(from -> criteriaBuilder.greaterThanOrEqualTo(root.get("createdAt"), from));
	}

	private Optional<Predicate> createdAtLessThanOrEqual(
			DlqMessageSearchCondition condition,
			Root<DlqMessage> root,
			CriteriaBuilder criteriaBuilder) {
		return Optional.ofNullable(condition.to())
				.map(to -> criteriaBuilder.lessThanOrEqualTo(root.get("createdAt"), to));
	}

	private boolean hasNoText(String value) {
		return value == null || value.isBlank();
	}

}
