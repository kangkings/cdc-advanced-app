package com.practice.datatransformer.oracle;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class SourceTableMappingRegistry {

	private final Map<String, SourceTableMapping> mappings;

	// 설정된 테이블 매핑을 한 번만 로드해 런타임 조회 단순화
	public SourceTableMappingRegistry(SourceTableMappingProperties properties) {
		this.mappings = properties.tables().stream()
				.collect(Collectors.toUnmodifiableMap(
						mapping -> normalize(mapping.tableName()),
						Function.identity()));
		log.info("[SOURCE-TABLE-MAPPING][LOADED] count={}, tables={}", mappings.size(), mappings.keySet());
	}

	// 검증과 payload 파싱에 사용할 원본 테이블 메타데이터 조회
	public Optional<SourceTableMapping> find(String tableName) {
		if (tableName == null || tableName.isBlank()) {
			return Optional.empty();
		}
		return Optional.ofNullable(mappings.get(normalize(tableName)));
	}

	private String normalize(String tableName) {
		return tableName.toUpperCase(Locale.ROOT);
	}

}
