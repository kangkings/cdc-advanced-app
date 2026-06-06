package com.practice.datatransformer.oracle;

import java.util.List;

public record TableRule(
		String owner,
		String tableName,
		String keyColumn,
		List<String> insertColumns) {

	public String qualifiedTableName() {
		return "%s.%s".formatted(owner, tableName);
	}

}
