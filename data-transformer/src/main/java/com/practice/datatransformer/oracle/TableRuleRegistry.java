package com.practice.datatransformer.oracle;

import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.stereotype.Component;

@Component
public class TableRuleRegistry {

	private final Map<String, TableRule> rules = Map.of(
			"P_USERS", new TableRule(
					"DATA_GENERATOR",
					"P_USERS",
					"ID",
					List.of("NAME", "EMAIL", "STATUS", "CREATED_AT", "UPDATED_AT")));

	public Optional<TableRule> find(String tableName) {
		if (tableName == null || tableName.isBlank()) {
			return Optional.empty();
		}
		return Optional.ofNullable(rules.get(tableName.toUpperCase(Locale.ROOT)));
	}

}
