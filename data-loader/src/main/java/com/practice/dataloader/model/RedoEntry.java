package com.practice.dataloader.model;

import java.sql.Timestamp;

// transformer가 전달한 원본 redo log 데이터
public record RedoEntry(
		int rowNumber,
		long scn,
		Timestamp timestamp,
		String operation,
		String owner,
		String tableName,
		String username,
		String rowId,
		String sqlRedo) {
}
