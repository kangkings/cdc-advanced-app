package com.practice.dataloader.pipeline;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class LoaderTableMappingRegistry {

	private final Map<String, LoaderTableMapping> mappings;

	// 설정된 적재 테이블 매핑을 한 번만 로드해 런타임 조회 단순화
	public LoaderTableMappingRegistry(LoaderTableMappingProperties properties) {
		this.mappings = properties.tables().stream()
				.collect(Collectors.toUnmodifiableMap(
						mapping -> normalize(mapping.sourceTable()),
						Function.identity()));
		log.info("[LOADER-TABLE-MAPPING][LOADED] count={}, sourceTables={}", mappings.size(), mappings.keySet());
	}

	// payload의 source table에 맞는 적재 매핑 조회
	public Optional<LoaderTableMapping> find(String sourceTable) {
		if (sourceTable == null || sourceTable.isBlank()) {
			return Optional.empty();
		}
		return Optional.ofNullable(mappings.get(normalize(sourceTable)));
	}

	private String normalize(String tableName) {
		return tableName.toUpperCase(Locale.ROOT);
	}

}
