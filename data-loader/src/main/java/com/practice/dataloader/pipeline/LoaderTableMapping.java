package com.practice.dataloader.pipeline;

import java.util.List;

// source table과 MySQL target table 적재 매핑
public record LoaderTableMapping(
		String sourceTable,
		String targetTable,
		String keyColumn,
		List<String> requiredInsertColumns) {

	public LoaderTableMapping {
		requiredInsertColumns = requiredInsertColumns == null ? List.of() : List.copyOf(requiredInsertColumns);
	}

}
