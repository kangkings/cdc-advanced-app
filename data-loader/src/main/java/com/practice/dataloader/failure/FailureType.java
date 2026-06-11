package com.practice.dataloader.failure;

// data-loader 비재시도 실패 유형 분류
public enum FailureType {

	// TransformEvent JSON 역직렬화 실패
	DESERIALIZATION_FAILED,
	// 적재 대상에서 제외된 source table
	UNSUPPORTED_TABLE,
	// 적재 대상에서 제외된 CDC operation
	UNSUPPORTED_OPERATION,
	// insert payload 필수 컬럼 누락
	MISSING_REQUIRED_FIELD,
	// payload key 기준 컬럼 누락
	MISSING_KEY,
	// 숫자 등 타입 변환 실패
	TYPE_CONVERSION_FAILED,
	// timestamp 변환 실패
	TIMESTAMP_PARSE_FAILED,
	// MySQL 쓰기 실패
	MYSQL_WRITE_FAILED,
	// 분류되지 않은 적재 실패
	UNKNOWN_LOAD_FAILED

}
