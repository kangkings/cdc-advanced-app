package com.practice.dataloader.model;

import java.util.Map;

// 적재 대상 테이블 key와 변경 컬럼 payload
public record RowPayload(
		String tableName,
		String operation,
		Map<String, Object> key,
		Map<String, Object> data) {
}
