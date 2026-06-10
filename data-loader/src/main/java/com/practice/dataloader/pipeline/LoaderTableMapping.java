package com.practice.dataloader.pipeline;

import java.util.List;

public record LoaderTableMapping(
		String sourceTable,
		String targetTable,
		String keyColumn,
		List<String> requiredInsertColumns) {

	public LoaderTableMapping {
		requiredInsertColumns = requiredInsertColumns == null ? List.of() : List.copyOf(requiredInsertColumns);
	}

}
