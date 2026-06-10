package com.practice.datatransformer.oracle;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

// 지원할 CDC 원본 테이블을 설정으로 확장할 수 있게 매핑 정보를 바인딩한다.
@ConfigurationProperties(prefix = "cdc.source")
public record SourceTableMappingProperties(List<SourceTableMapping> tables) {

	public SourceTableMappingProperties {
		tables = tables == null ? List.of() : List.copyOf(tables);
	}

}
