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

	// Loads configured table mappings once so runtime lookup stays simple and fast.
	public SourceTableMappingRegistry(SourceTableMappingProperties properties) {
		this.mappings = properties.tables().stream()
				.collect(Collectors.toUnmodifiableMap(
						mapping -> normalize(mapping.tableName()),
						Function.identity()));
		log.info("[SOURCE-TABLE-MAPPING][LOADED] count={}, tables={}", mappings.size(), mappings.keySet());
	}

	// Resolves configured source table metadata before validation and payload parsing.
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
