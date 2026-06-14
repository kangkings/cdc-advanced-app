package com.practice.logscanner.routing;

import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.stereotype.Component;

import com.practice.logscanner.batch.model.RedoLogEntry;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
// 명시적 table rule 기반 Kafka routing key 생성
public class RoutingKeyResolver {

	private final RoutingProperties routingProperties;

	public RoutingKeyResolver(RoutingProperties routingProperties) {
		this.routingProperties = routingProperties;
	}

	// aggregate key를 만들 수 없으면 기존 SCN 기반 key로 fallback
	public String resolve(RedoLogEntry entry) {
		return findRule(entry)
				.flatMap(rule -> extractKey(entry.sqlRedo(), rule.aggregateKeyColumn())
						.map(value -> "%s:%s".formatted(rule.aggregateName(), value)))
				.orElseGet(() -> fallbackKey(entry));
	}

	private Optional<RoutingProperties.TableRule> findRule(RedoLogEntry entry) {
		if (entry == null || entry.tableName() == null) {
			return Optional.empty();
		}
		String tableName = entry.tableName().toUpperCase(Locale.ROOT);
		return routingProperties.tables().stream()
				.filter(rule -> rule.tableName() != null && tableName.equals(rule.tableName().toUpperCase(Locale.ROOT)))
				.findFirst();
	}

	private Optional<String> extractKey(String sqlRedo, String column) {
		if (sqlRedo == null || column == null || column.isBlank()) {
			return Optional.empty();
		}
		String quotedColumn = Pattern.quote(column);
		Pattern assignmentPattern = Pattern.compile("(?is)\"?" + quotedColumn + "\"?\\s*=\\s*'?([^',;\\s]+)'?");
		Matcher matcher = assignmentPattern.matcher(sqlRedo);
		if (!matcher.find()) {
			log.warn("[ROUTING][KEY][FALLBACK] aggregateKeyColumn={} 값을 sqlRedo에서 찾을 수 없음", column);
			return Optional.empty();
		}
		return Optional.of(matcher.group(1));
	}

	private String fallbackKey(RedoLogEntry entry) {
		return "%d:%d".formatted(entry.scn(), entry.rowNumber());
	}

}
