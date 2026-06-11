package com.practice.datatransformer.oracle;

import java.util.List;

// Oracle source table 검증과 payload 파싱 매핑
public record SourceTableMapping(
		String owner,
		String tableName,
		String keyColumn,
		List<String> insertColumns) {

	// owner와 tableName을 합친 Oracle 식별자 생성
	public String qualifiedTableName() {
		return "%s.%s".formatted(owner, tableName);
	}

}
