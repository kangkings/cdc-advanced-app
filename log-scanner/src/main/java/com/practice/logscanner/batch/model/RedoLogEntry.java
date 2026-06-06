package com.practice.logscanner.batch.model;

import java.sql.Timestamp;

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
