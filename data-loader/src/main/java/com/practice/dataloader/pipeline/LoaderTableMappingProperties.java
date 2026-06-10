package com.practice.dataloader.pipeline;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

// 설정 기반 적재 대상 테이블 매핑 바인딩
@ConfigurationProperties(prefix = "cdc.loader")
public record LoaderTableMappingProperties(List<LoaderTableMapping> tables) {

	public LoaderTableMappingProperties {
		tables = tables == null ? List.of() : List.copyOf(tables);
	}

}
