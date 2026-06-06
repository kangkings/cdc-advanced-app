package com.practice.dataloader.model;

import java.util.Map;

public record RowPayload(
		String tableName,
		String operation,
		Map<String, Object> key,
		Map<String, Object> data) {
}
