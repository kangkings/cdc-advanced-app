package com.practice.datatransformer.oracle;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.stereotype.Component;

import com.practice.datatransformer.model.RedoEntry;
import com.practice.datatransformer.model.RowPayload;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class RedoSqlParser {

	private static final DateTimeFormatter ORACLE_LOGMINER_TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
			.appendPattern("yy/MM/dd HH:mm:ss")
			.optionalStart()
			.appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
			.optionalEnd()
			.toFormatter();

	public Optional<Long> extractKey(String sqlRedo, String keyColumn) {
		if (sqlRedo == null || sqlRedo.isBlank()) {
			return Optional.empty();
		}
		if (keyColumn == null || keyColumn.isBlank()) {
			return Optional.empty();
		}

		String quotedColumn = Pattern.quote(keyColumn);
		Pattern wherePattern = Pattern.compile("(?i)\\bwhere\\b.*?\\b" + quotedColumn + "\\b\\s*=\\s*(\\d+)");
		Pattern insertPattern = Pattern.compile("(?i)\\b" + quotedColumn + "\\b\\s*,.*?\\bvalues\\s*\\(\\s*(\\d+)");

		return findLong(wherePattern, sqlRedo)
				.or(() -> findLong(insertPattern, sqlRedo));
	}

	public RowPayload parsePayload(RedoEntry entry, SourceTableMapping mapping, Long keyValue) {
		return new RowPayload(
				mapping.tableName(),
				entry.operation().toUpperCase(Locale.ROOT),
				Map.of(mapping.keyColumn(), keyValue),
				parseData(entry.sqlRedo(), entry.operation(), mapping));
	}

	private Map<String, Object> parseData(String sqlRedo, String operation, SourceTableMapping mapping) {
		if (operation == null) {
			return Map.of();
		}

		return switch (operation.toUpperCase(Locale.ROOT)) {
			case "INSERT" -> parseInsertValues(sqlRedo, mapping);
			case "UPDATE" -> parseUpdateValues(sqlRedo);
			default -> Map.of();
		};
	}

	private Map<String, Object> parseInsertValues(String sqlRedo, SourceTableMapping mapping) {
		int valuesIndex = indexOfKeyword(sqlRedo, "values");
		if (valuesIndex < 0) {
			log.warn("[REDO-SQL-PARSER][INSERT] VALUES keyword was not found. sqlRedo={}", abbreviate(sqlRedo));
			return Map.of();
		}

		String beforeValues = sqlRedo.substring(0, valuesIndex);
		String afterValues = sqlRedo.substring(valuesIndex + "values".length());
		Map<String, Object> assignmentData = parseAssignments(afterValues);
		if (!assignmentData.isEmpty()) {
			return assignmentData;
		}

		String columnGroup = extractLastParenthesizedGroup(beforeValues);
		String valueGroup = extractFirstParenthesizedGroup(afterValues);
		if (valueGroup == null) {
			log.warn("[REDO-SQL-PARSER][INSERT] values group was not found. sqlRedo={}", abbreviate(sqlRedo));
			return Map.of();
		}

		List<String> columns = columnGroup == null || looksLikeTableNameGroup(columnGroup)
				? mapping.insertColumns()
				: splitTopLevel(columnGroup);
		List<String> values = splitTopLevel(valueGroup);
		if (columns.isEmpty()) {
			log.warn("[REDO-SQL-PARSER][INSERT] columns were not found. sqlRedo={}", abbreviate(sqlRedo));
			return Map.of();
		}

		Map<String, Object> data = new LinkedHashMap<>();
		for (int i = 0; i < Math.min(columns.size(), values.size()); i++) {
			data.put(normalizeIdentifier(columns.get(i)), normalizeValue(values.get(i)));
		}
		if (data.isEmpty()) {
			log.warn("[REDO-SQL-PARSER][INSERT] no data parsed. columns={}, values={}, sqlRedo={}",
					columns,
					values,
					abbreviate(sqlRedo));
		}
		return data;
	}

	private Map<String, Object> parseAssignments(String value) {
		Map<String, Object> data = new LinkedHashMap<>();
		for (String assignment : splitTopLevel(trimTrailingStatement(value))) {
			int index = indexOfTopLevelEquals(assignment);
			if (index < 0) {
				continue;
			}
			data.put(
					normalizeIdentifier(assignment.substring(0, index)),
					normalizeValue(assignment.substring(index + 1)));
		}
		return data;
	}

	private boolean looksLikeTableNameGroup(String value) {
		return splitTopLevel(value).size() == 1 && !value.contains(",");
	}

	private int indexOfKeyword(String value, String keyword) {
		Matcher matcher = Pattern.compile("(?i)\\b" + Pattern.quote(keyword) + "\\b").matcher(value);
		return matcher.find() ? matcher.start() : -1;
	}

	private String extractLastParenthesizedGroup(String value) {
		int end = -1;
		int depth = 0;
		boolean quoted = false;

		for (int i = value.length() - 1; i >= 0; i--) {
			char ch = value.charAt(i);
			if (ch == '\'') {
				quoted = !quoted;
				continue;
			}
			if (quoted) {
				continue;
			}
			if (ch == ')') {
				if (depth == 0) {
					end = i;
				}
				depth++;
			}
			else if (ch == '(') {
				depth--;
				if (depth == 0 && end >= 0) {
					return value.substring(i + 1, end);
				}
			}
		}
		return null;
	}

	private String extractFirstParenthesizedGroup(String value) {
		int start = -1;
		int depth = 0;
		boolean quoted = false;

		for (int i = 0; i < value.length(); i++) {
			char ch = value.charAt(i);
			if (ch == '\'') {
				if (quoted && i + 1 < value.length() && value.charAt(i + 1) == '\'') {
					i++;
					continue;
				}
				quoted = !quoted;
				continue;
			}
			if (quoted) {
				continue;
			}
			if (ch == '(') {
				if (depth == 0) {
					start = i;
				}
				depth++;
			}
			else if (ch == ')') {
				depth--;
				if (depth == 0 && start >= 0) {
					return value.substring(start + 1, i);
				}
			}
		}
		return null;
	}

	private Map<String, Object> parseUpdateValues(String sqlRedo) {
		Matcher matcher = Pattern.compile("(?is)\\bset\\b(.*?)\\bwhere\\b").matcher(sqlRedo);
		if (!matcher.find()) {
			return Map.of();
		}

		return parseAssignments(matcher.group(1));
	}

	private List<String> splitTopLevel(String value) {
		List<String> parts = new java.util.ArrayList<>();
		StringBuilder current = new StringBuilder();
		int depth = 0;
		boolean quoted = false;

		for (int i = 0; i < value.length(); i++) {
			char ch = value.charAt(i);
			if (ch == '\'') {
				current.append(ch);
				if (quoted && i + 1 < value.length() && value.charAt(i + 1) == '\'') {
					current.append(value.charAt(++i));
					continue;
				}
				quoted = !quoted;
				continue;
			}
			if (!quoted && ch == '(') {
				depth++;
			}
			else if (!quoted && ch == ')') {
				depth--;
			}
			else if (!quoted && depth == 0 && ch == ',') {
				parts.add(current.toString().trim());
				current.setLength(0);
				continue;
			}
			current.append(ch);
		}

		if (!current.isEmpty()) {
			parts.add(current.toString().trim());
		}
		return parts;
	}

	private int indexOfTopLevelEquals(String value) {
		boolean quoted = false;
		for (int i = 0; i < value.length(); i++) {
			char ch = value.charAt(i);
			if (ch == '\'') {
				if (quoted && i + 1 < value.length() && value.charAt(i + 1) == '\'') {
					i++;
					continue;
				}
				quoted = !quoted;
			}
			else if (!quoted && ch == '=') {
				return i;
			}
		}
		return -1;
	}

	private String normalizeIdentifier(String value) {
		String normalized = value.trim();
		int dotIndex = normalized.lastIndexOf('.');
		if (dotIndex >= 0) {
			normalized = normalized.substring(dotIndex + 1);
		}
		return normalized.replace("\"", "").trim().toUpperCase(Locale.ROOT);
	}

	private Object normalizeValue(String value) {
		String normalized = trimTrailingStatement(value.trim());
		if (normalized.equalsIgnoreCase("NULL")) {
			return null;
		}

		Matcher functionLiteral = Pattern.compile("(?is)\\w+\\s*\\(\\s*'((?:''|[^'])*)'").matcher(normalized);
		if (functionLiteral.find()) {
			return unquote(functionLiteral.group(1));
		}
		if (normalized.startsWith("'") && normalized.endsWith("'")) {
			return normalizeStringLiteral(unquote(normalized.substring(1, normalized.length() - 1)));
		}
		if (normalized.matches("-?\\d+")) {
			return Long.parseLong(normalized);
		}
		return normalized;
	}

	private Object normalizeStringLiteral(String value) {
		try {
			return LocalDateTime.parse(value, ORACLE_LOGMINER_TIMESTAMP_FORMATTER).toString();
		}
		catch (DateTimeParseException ignored) {
			return value;
		}
	}

	private String trimTrailingStatement(String value) {
		String trimmed = value.trim();
		while (trimmed.endsWith(";")) {
			trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
		}
		return trimmed;
	}

	private String unquote(String value) {
		return value.replace("''", "'");
	}

	private String abbreviate(String value) {
		if (value == null || value.length() <= 500) {
			return value;
		}
		return value.substring(0, 500) + "...";
	}

	private Optional<Long> findLong(Pattern pattern, String value) {
		Matcher matcher = pattern.matcher(value);
		if (!matcher.find()) {
			return Optional.empty();
		}
		return Optional.of(Long.parseLong(matcher.group(1)));
	}

}
