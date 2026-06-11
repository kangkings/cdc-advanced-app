package com.practice.datatransformer.model;

import java.sql.Timestamp;

// log-scanner가 발행한 원본 redo log 데이터
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
