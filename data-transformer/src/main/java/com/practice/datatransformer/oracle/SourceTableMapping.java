package com.practice.datatransformer.oracle;

import java.util.List;

public record SourceTableMapping(
		String owner,
		String tableName,
		String keyColumn,
		List<String> insertColumns) {

	public String qualifiedTableName() {
		return "%s.%s".formatted(owner, tableName);
	}

}
