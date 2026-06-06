package com.practice.datatransformer.pipeline;

import java.util.Locale;
import java.util.Set;

import org.springframework.stereotype.Service;

import com.practice.datatransformer.model.CheckResult;
import com.practice.datatransformer.model.RedoEntry;
import com.practice.datatransformer.model.RowPayload;
import com.practice.datatransformer.oracle.RedoSqlParser;
import com.practice.datatransformer.oracle.RowLookup;
import com.practice.datatransformer.oracle.TableRule;
import com.practice.datatransformer.oracle.TableRuleRegistry;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class ValidationService {

	private static final String MODULE = "data-transformer";
	private static final Set<String> SUPPORTED_OPERATIONS = Set.of("INSERT", "UPDATE", "DELETE");
	private static final String ROW_LOOKUP_DURATION = "data_transformer.oracle.row_lookup.duration";
	private static final String ROW_LOOKUP_COUNT = "data_transformer.oracle.row_lookup.count";

	private final RedoSqlParser redoSqlParser;
	private final RowLookup rowLookup;
	private final TableRuleRegistry tableRuleRegistry;
	private final MeterRegistry meterRegistry;

	public CheckResult check(RedoEntry entry) {
		if (entry == null) {
			return CheckResult.invalid("entry is null");
		}
		if (entry.operation() == null || entry.tableName() == null || entry.sqlRedo() == null || entry.rowId() == null) {
			return CheckResult.invalid("operation, tableName, rowId, and sqlRedo are required");
		}

		String operation = entry.operation().toUpperCase(Locale.ROOT);
		String tableName = entry.tableName().toUpperCase(Locale.ROOT);

		if (!SUPPORTED_OPERATIONS.contains(operation)) {
			return CheckResult.unsupported("unsupported operation: " + entry.operation());
		}

		return tableRuleRegistry.find(tableName)
				.map(rule -> checkRow(entry, rule, operation))
				.orElseGet(() -> CheckResult.unsupported("unsupported table: " + entry.tableName()));
	}

	private CheckResult checkRow(RedoEntry entry, TableRule rule, String operation) {
		return findKey(rule, entry.rowId(), operation)
				.map(keyValue -> CheckResult.valid(true, keyValue))
				.orElseGet(() -> CheckResult.invalid("%s was not found by ROWID".formatted(rule.keyColumn())));
	}

	public RowPayload payload(RedoEntry entry, CheckResult checkResult) {
		if (entry == null || entry.tableName() == null || entry.sqlRedo() == null || checkResult == null
				|| !checkResult.valid() || checkResult.rowId() == null) {
			return null;
		}
		return tableRuleRegistry.find(entry.tableName())
				.map(rule -> redoSqlParser.parsePayload(entry, rule, checkResult.rowId()))
				.orElse(null);
	}

	private java.util.Optional<Long> findKey(TableRule rule, String rowId, String operation) {
		Timer.Sample sample = Timer.start(meterRegistry);
		java.util.Optional<Long> keyValue = rowLookup.findKeyByRowId(rule, rowId);
		sample.stop(Timer.builder(ROW_LOOKUP_DURATION)
				.description("Oracle row key lookup duration")
				.tag("module", MODULE)
				.tag("table", rule.tableName())
				.tag("operation", operation)
				.register(meterRegistry));
		meterRegistry.counter(ROW_LOOKUP_COUNT, "module", MODULE, "table", rule.tableName(), "operation", operation)
				.increment();
		return keyValue;
	}

}
