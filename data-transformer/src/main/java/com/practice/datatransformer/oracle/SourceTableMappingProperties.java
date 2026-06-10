package com.practice.datatransformer.oracle;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

// 설정 기반 CDC 원본 테이블 매핑 바인딩
@ConfigurationProperties(prefix = "cdc.source")
public record SourceTableMappingProperties(List<SourceTableMapping> tables) {

	public SourceTableMappingProperties {
		tables = tables == null ? List.of() : List.copyOf(tables);
	}

}
