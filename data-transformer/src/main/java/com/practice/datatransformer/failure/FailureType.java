package com.practice.datatransformer.failure;

// data-transformer 비재시도 실패 유형 분류
public enum FailureType {

	// RedoEntry JSON 역직렬화 실패
	DESERIALIZATION_FAILED,
	// 변환 대상에서 제외된 source table
	UNSUPPORTED_TABLE,
	// 변환 대상에서 제외된 CDC operation
	UNSUPPORTED_OPERATION,
	// 변환 payload 필수값 누락
	MISSING_REQUIRED_FIELD,
	// source row key 조회 실패
	SOURCE_KEY_LOOKUP_FAILED,
	// Oracle 객체 또는 권한 문제
	ORACLE_OBJECT_ACCESS_FAILED,
	// 재시도 횟수 소진
	RETRY_EXHAUSTED,
	// sqlRedo payload 파싱 실패
	PAYLOAD_PARSE_FAILED,
	// 분류되지 않은 변환 실패
	UNKNOWN_TRANSFORM_FAILED

}
