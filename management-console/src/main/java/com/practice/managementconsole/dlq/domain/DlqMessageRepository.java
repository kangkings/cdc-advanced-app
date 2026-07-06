package com.practice.managementconsole.dlq.domain;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

public interface DlqMessageRepository extends JpaRepository<DlqMessage, Long>, JpaSpecificationExecutor<DlqMessage> {

	// 원본 메시지 위치 기준 중복 수집 확인
	boolean existsBySourceTopicAndSourcePartitionAndSourceOffsetAndStage(
			String sourceTopic,
			Integer sourcePartition,
			Long sourceOffset,
			String stage);

	// 원본 메시지 위치 기준 저장 메시지 조회
	Optional<DlqMessage> findBySourceTopicAndSourcePartitionAndSourceOffsetAndStage(
			String sourceTopic,
			Integer sourcePartition,
			Long sourceOffset,
			String stage);

}
