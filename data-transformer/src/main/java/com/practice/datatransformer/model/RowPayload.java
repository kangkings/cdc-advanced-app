package com.practice.datatransformer.model;

import java.util.Map;

// sqlRedo에서 추출한 key와 변경 컬럼 payload
public record RowPayload(
		String tableName,
		String operation,
		Map<String, Object> key,
		Map<String, Object> data) {
}
