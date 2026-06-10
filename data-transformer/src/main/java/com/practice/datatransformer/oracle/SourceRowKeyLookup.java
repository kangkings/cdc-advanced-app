package com.practice.datatransformer.oracle;

import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class SourceRowKeyLookup {

	private final JdbcTemplate jdbcTemplate;

	public Optional<Long> findKeyByRowId(SourceTableMapping mapping, String rowId) {
		validateIdentifier(mapping.owner());
		validateIdentifier(mapping.tableName());
		validateIdentifier(mapping.keyColumn());
		if (rowId == null || rowId.isBlank()) {
			return Optional.empty();
		}

		return jdbcTemplate.query(
				"SELECT %s FROM %s WHERE ROWID = CHARTOROWID(?)"
						.formatted(mapping.keyColumn(), mapping.qualifiedTableName()),
				resultSet -> {
					if (!resultSet.next()) {
						return Optional.empty();
					}
					return Optional.of(resultSet.getLong(mapping.keyColumn()));
				},
				rowId);
	}

	private void validateIdentifier(String identifier) {
		if (identifier == null || !identifier.matches("[A-Z][A-Z0-9_]*")) {
			throw new IllegalArgumentException("Invalid Oracle identifier: " + identifier);
		}
	}

}
