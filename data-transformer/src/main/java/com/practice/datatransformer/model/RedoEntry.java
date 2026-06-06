package com.practice.datatransformer.model;

import java.sql.Timestamp;

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
