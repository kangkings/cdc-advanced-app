package com.practice.logscanner.batch.model;

import java.sql.Timestamp;

// LogMiner에서 읽은 redo log 한 건
public record RedoLogEntry(
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
