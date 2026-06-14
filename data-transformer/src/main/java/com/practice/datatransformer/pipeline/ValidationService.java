package com.practice.datatransformer.pipeline;

import java.util.Locale;
import java.util.Set;

import org.springframework.stereotype.Service;

import com.practice.datatransformer.model.CheckResult;
import com.practice.datatransformer.model.RedoEntry;
import com.practice.datatransformer.model.RowPayload;
import com.practice.datatransformer.oracle.RedoSqlParser;
import com.practice.datatransformer.oracle.SourceRowKeyLookup;
import com.practice.datatransformer.oracle.SourceTableMapping;
import com.practice.datatransformer.oracle.SourceTableMappingRegistry;
import com.practice.datatransformer.observability.TransformerMetrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
// source row 존재 여부와 payload 파싱 검증
public class ValidationService {

	private static final Set<String> SUPPORTED_OPERATIONS = Set.of("INSERT", "UPDATE", "DELETE");

	private final RedoSqlParser redoSqlParser;
	private final SourceRowKeyLookup sourceRowKeyLookup;
	private final SourceTableMappingRegistry sourceTableMappingRegistry;
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

		return sourceTableMappingRegistry.find(tableName)
				.map(mapping -> checkRow(entry, mapping, operation))
				.orElseGet(() -> CheckResult.unsupported("unsupported table: " + entry.tableName()));
	}

	private CheckResult checkRow(RedoEntry entry, SourceTableMapping mapping, String operation) {
		java.util.Optional<Long> rowKey = findKey(mapping, entry.rowId(), operation);
		if (rowKey.isPresent()) {
			return CheckResult.valid(true, rowKey.get());
		}

		return extractKey(entry, mapping)
				.map(keyValue -> CheckResult.valid(false, keyValue))
				.orElseGet(() -> CheckResult.invalid("%s was not found by ROWID or sqlRedo".formatted(mapping.keyColumn())));
	}

	public RowPayload payload(RedoEntry entry, CheckResult checkResult) {
		if (entry == null || entry.tableName() == null || entry.sqlRedo() == null || checkResult == null
				|| !checkResult.valid() || checkResult.sourceKeyValue() == null) {
			return null;
		}
		return sourceTableMappingRegistry.find(entry.tableName())
				.map(mapping -> redoSqlParser.parsePayload(entry, mapping, checkResult.sourceKeyValue()))
				.orElse(null);
	}

	private java.util.Optional<Long> findKey(SourceTableMapping mapping, String rowId, String operation) {
		Timer.Sample sample = Timer.start(meterRegistry);
		java.util.Optional<Long> keyValue = sourceRowKeyLookup.findKeyByRowId(mapping, rowId);
		sample.stop(Timer.builder(TransformerMetrics.Names.ORACLE_ROW_LOOKUP_DURATION)
				.description("Oracle row key lookup duration")
				.tag(TransformerMetrics.Tags.MODULE, TransformerMetrics.MODULE)
				.tag(TransformerMetrics.Tags.TABLE, mapping.tableName())
				.tag(TransformerMetrics.Tags.OPERATION, operation)
				.register(meterRegistry));
		meterRegistry.counter(
				TransformerMetrics.Names.ORACLE_ROW_LOOKUP_COUNT,
				TransformerMetrics.Tags.MODULE, TransformerMetrics.MODULE,
				TransformerMetrics.Tags.TABLE, mapping.tableName(),
				TransformerMetrics.Tags.OPERATION, operation)
				.increment();
		return keyValue;
	}

	// ROWID 조회 실패 시 DELETE처럼 원본 row가 사라진 이벤트의 key를 sqlRedo에서 보완
	private java.util.Optional<Long> extractKey(RedoEntry entry, SourceTableMapping mapping) {
		return redoSqlParser.extractKey(entry.sqlRedo(), mapping.keyColumn());
	}

}
