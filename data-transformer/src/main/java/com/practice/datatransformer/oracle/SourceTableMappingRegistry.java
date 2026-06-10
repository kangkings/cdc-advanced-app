package com.practice.datatransformer.oracle;

import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.stereotype.Component;

@Component
public class SourceTableMappingRegistry {

	private final Map<String, SourceTableMapping> mappings = Map.of(
			"P_USERS", new SourceTableMapping(
					"DATA_GENERATOR",
					"P_USERS",
					"ID",
					List.of("NAME", "EMAIL", "STATUS", "CREATED_AT", "UPDATED_AT")));

	public Optional<SourceTableMapping> find(String tableName) {
		if (tableName == null || tableName.isBlank()) {
			return Optional.empty();
		}
		return Optional.ofNullable(mappings.get(tableName.toUpperCase(Locale.ROOT)));
	}

}
