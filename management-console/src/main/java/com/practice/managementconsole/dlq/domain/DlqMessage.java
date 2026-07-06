package com.practice.managementconsole.dlq.domain;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(
		name = "dlq_message",
		uniqueConstraints = @UniqueConstraint(
				name = "uk_dlq_message_source",
				columnNames = {"source_topic", "source_partition", "source_offset", "stage"}))
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class DlqMessage {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(nullable = false, length = 100)
	private String stage;

	@Column(name = "failure_type", nullable = false, length = 100)
	private String failureType;

	@Column(nullable = false, length = 1000)
	private String reason;

	@Column(nullable = false)
	private boolean retryable;

	@Column(name = "source_topic", nullable = false, length = 200)
	private String sourceTopic;

	@Column(name = "source_partition", nullable = false)
	private Integer sourcePartition;

	@Column(name = "source_offset", nullable = false)
	private Long sourceOffset;

	@Lob
	@Column(name = "original_message")
	private String originalMessage;

	@Lob
	@Column(name = "raw_dlq_message", nullable = false)
	private String rawDlqMessage;

	@Enumerated(EnumType.STRING)
	@Column(nullable = false, length = 30)
	private DlqMessageStatus status;

	@Column(name = "replay_topic", length = 200)
	private String replayTopic;

	@Column(length = 1000)
	private String memo;

	@Column(name = "created_at", nullable = false)
	private LocalDateTime createdAt;

	@Column(name = "updated_at", nullable = false)
	private LocalDateTime updatedAt;

	@Column(name = "replayed_at")
	private LocalDateTime replayedAt;

	@Column(name = "ignored_at")
	private LocalDateTime ignoredAt;

	@Column(name = "completed_at")
	private LocalDateTime completedAt;

	private DlqMessage(
			String stage,
			String failureType,
			String reason,
			boolean retryable,
			String sourceTopic,
			Integer sourcePartition,
			Long sourceOffset,
			String originalMessage,
			String rawDlqMessage) {
		this.stage = required(stage, "stage");
		this.failureType = required(failureType, "failureType");
		this.reason = required(reason, "reason");
		this.retryable = retryable;
		this.sourceTopic = required(sourceTopic, "sourceTopic");
		this.sourcePartition = required(sourcePartition, "sourcePartition");
		this.sourceOffset = required(sourceOffset, "sourceOffset");
		this.originalMessage = originalMessage;
		this.rawDlqMessage = required(rawDlqMessage, "rawDlqMessage");
		this.status = DlqMessageStatus.NEW;
	}

	// DLQ 수집 메시지 생성
	public static DlqMessage create(
			String stage,
			String failureType,
			String reason,
			boolean retryable,
			String sourceTopic,
			Integer sourcePartition,
			Long sourceOffset,
			String originalMessage,
			String rawDlqMessage) {
		return new DlqMessage(
				stage,
				failureType,
				reason,
				retryable,
				sourceTopic,
				sourcePartition,
				sourceOffset,
				originalMessage,
				rawDlqMessage);
	}

	// replay 발행 완료 상태 전환
	public void markReplayed(String replayTopic, String memo) {
		validateNewStatus();
		this.status = DlqMessageStatus.REPLAYED;
		this.replayTopic = required(replayTopic, "replayTopic");
		this.memo = memo;
		this.replayedAt = LocalDateTime.now();
	}

	// 운영자 무시 상태 전환
	public void ignore(String reason) {
		validateNewStatus();
		this.status = DlqMessageStatus.IGNORED;
		this.memo = required(reason, "reason");
		this.ignoredAt = LocalDateTime.now();
	}

	// 운영자 수동 완료 상태 전환
	public void complete(String memo) {
		validateNewStatus();
		this.status = DlqMessageStatus.COMPLETED;
		this.memo = required(memo, "memo");
		this.completedAt = LocalDateTime.now();
	}

	// 생성/수정 시각 초기화
	@PrePersist
	void prePersist() {
		LocalDateTime now = LocalDateTime.now();
		this.createdAt = now;
		this.updatedAt = now;
	}

	// 수정 시각 갱신
	@PreUpdate
	void preUpdate() {
		this.updatedAt = LocalDateTime.now();
	}

	private void validateNewStatus() {
		if (status != DlqMessageStatus.NEW) {
			throw new IllegalStateException("이미 처리된 DLQ 메시지입니다. status=" + status);
		}
	}

	private <T> T required(T value, String fieldName) {
		if (value == null) {
			throw new IllegalArgumentException(fieldName + " 값이 필요합니다.");
		}
		if (value instanceof String stringValue && stringValue.isBlank()) {
			throw new IllegalArgumentException(fieldName + " 값이 필요합니다.");
		}
		return value;
	}

}
