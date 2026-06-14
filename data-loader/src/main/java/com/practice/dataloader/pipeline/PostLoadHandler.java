package com.practice.dataloader.pipeline;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.practice.dataloader.failure.FailureType;
import com.practice.dataloader.failure.LoadNonRetryableException;
import com.practice.dataloader.model.RowPayload;
import com.practice.dataloader.model.TransformEvent;
import com.practice.dataloader.observability.LoaderMetrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
// P_POST payload를 MySQL p_post에 적재하는 handler
public class PostLoadHandler implements LoadHandler {

	private static final String TARGET_TABLE = "p_post";
	private static final String USER_ID = "USER_ID";
	private static final String TITLE = "TITLE";
	private static final String CONTENT = "CONTENT";
	private static final String STATUS = "STATUS";
	private static final String CREATED_AT = "CREATED_AT";
	private static final String UPDATED_AT = "UPDATED_AT";
	private static final List<String> UPDATE_COLUMNS = List.of(USER_ID, TITLE, CONTENT, STATUS, CREATED_AT, UPDATED_AT);
	private static final DateTimeFormatter ORACLE_LOGMINER_TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
			.appendPattern("yy/MM/dd HH:mm:ss")
			.optionalStart()
			.appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
			.optionalEnd()
			.toFormatter();

	private final JdbcTemplate jdbcTemplate;
	private final MeterRegistry meterRegistry;

	// 게시글 대상 테이블 매핑 처리 여부 확인
	@Override
	public boolean supports(LoaderTableMapping mapping) {
		return mapping != null && TARGET_TABLE.equalsIgnoreCase(mapping.targetTable());
	}

	// 게시글 처리기가 지원하는 CDC 작업 확인
	@Override
	public boolean supportsOperation(String operation) {
		if (operation == null) {
			return false;
		}
		return switch (operation.toUpperCase(Locale.ROOT)) {
			case "INSERT", "UPDATE", "DELETE" -> true;
			default -> false;
		};
	}

	@Override
	public boolean supportsBatchInsert(LoaderTableMapping mapping) {
		return supports(mapping);
	}

	// 게시글 payload의 key와 insert 필수 컬럼 검증
	@Override
	public void validate(RowPayload payload, LoaderTableMapping mapping) {
		validateKey(payload, mapping);
		if ("INSERT".equalsIgnoreCase(payload.operation())) {
			validateRequiredInsertColumns(payload, mapping);
			validateParentExists(payload, mapping);
		}
	}

	// 게시글 insert 이벤트를 MySQL 배치 insert로 적재
	@Override
	public void loadBatchInsert(List<TransformEvent> events, LoaderTableMapping mapping) {
		List<TransformEvent> insertEvents = events.stream()
				.peek(event -> validate(event.payload(), mapping))
				.toList();
		if (insertEvents.isEmpty()) {
			return;
		}

		LocalDateTime startTime = LocalDateTime.now();
		insertEvents.forEach(event -> logParentState(event.payload(), mapping));
		jdbcTemplate.batchUpdate("""
				INSERT INTO p_post (id, user_id, title, content, status, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?, ?, ?)
				""",
				insertEvents,
				insertEvents.size(),
				(ps, event) -> {
					Map<String, Object> data = event.payload().data();
					ps.setLong(1, asLong(event.payload().key().get(mapping.keyColumn())));
					ps.setLong(2, asLong(data.get(USER_ID)));
					ps.setString(3, asString(data.get(TITLE)));
					ps.setString(4, asString(data.get(CONTENT)));
					ps.setString(5, asString(data.get(STATUS)));
					ps.setObject(6, asLocalDateTime(data.get(CREATED_AT)));
					ps.setObject(7, asLocalDateTime(data.get(UPDATED_AT)));
				});

		Duration elapsed = Duration.between(startTime, LocalDateTime.now());
		Timer.builder(LoaderMetrics.Names.MYSQL_BATCH_INSERT_DURATION)
				.description("MySQL batch insert duration")
				.tag(LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE)
				.tag(LoaderMetrics.Tags.TABLE, mapping.sourceTable())
				.register(meterRegistry)
				.record(elapsed);
		meterRegistry.counter(
				LoaderMetrics.Names.MYSQL_BATCH_INSERT_COUNT,
				LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE,
				LoaderMetrics.Tags.TABLE, mapping.sourceTable())
				.increment(insertEvents.size());
		log.info("[LOAD][BATCH][DONE] table={}, operation=INSERT, batchSize={}, elapsedMs={}",
				mapping.sourceTable(),
				insertEvents.size(),
				elapsed.toMillis());
	}

	// 게시글 단건 INSERT/UPDATE/DELETE 분기 처리
	@Override
	public void load(RowPayload payload, LoaderTableMapping mapping) {
		switch (payload.operation().toUpperCase(Locale.ROOT)) {
			case "INSERT" -> insertPost(payload, mapping);
			case "UPDATE" -> updatePost(payload, mapping);
			case "DELETE" -> deletePost(payload, mapping);
			default -> throw nonRetryable(
					FailureType.UNSUPPORTED_OPERATION,
					"지원하지 않는 operation입니다",
					context(payload, mapping));
		}
	}

	private void insertPost(RowPayload payload, LoaderTableMapping mapping) {
		Long id = keyValue(payload, mapping);
		Map<String, Object> data = payload.data();
		validateRequiredInsertColumns(payload, mapping);
		validateParentExists(payload, mapping);

		Timer.Sample sample = Timer.start(meterRegistry);
		jdbcTemplate.update("""
				INSERT INTO p_post (id, user_id, title, content, status, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?, ?, ?)
				""",
				id,
				asLong(data.get(USER_ID)),
				asString(data.get(TITLE)),
				asString(data.get(CONTENT)),
				asString(data.get(STATUS)),
				asLocalDateTime(data.get(CREATED_AT)),
				asLocalDateTime(data.get(UPDATED_AT)));

		sample.stop(Timer.builder(LoaderMetrics.Names.MYSQL_INSERT_DURATION)
				.description("MySQL direct insert duration")
				.tag(LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE)
				.tag(LoaderMetrics.Tags.TABLE, mapping.sourceTable())
				.register(meterRegistry));
		meterRegistry.counter(
				LoaderMetrics.Names.MYSQL_INSERT_COUNT,
				LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE,
				LoaderMetrics.Tags.TABLE, mapping.sourceTable(),
				LoaderMetrics.Tags.STATUS, LoaderMetrics.Status.SUCCESS).increment();
		log.info("[LOAD][INSERT] table={}, id={}", mapping.sourceTable(), id);
	}

	private void updatePost(RowPayload payload, LoaderTableMapping mapping) {
		Long id = keyValue(payload, mapping);
		UpdateStatement statement = updateStatement(payload.data(), mapping, id);

		Timer.Sample sample = Timer.start(meterRegistry);
		int updated = jdbcTemplate.update(statement.sql(), statement.params().toArray());
		sample.stop(Timer.builder(LoaderMetrics.Names.MYSQL_UPDATE_DURATION)
				.description("MySQL direct update duration")
				.tag(LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE)
				.tag(LoaderMetrics.Tags.TABLE, mapping.sourceTable())
				.register(meterRegistry));
		log.info("[LOAD][UPDATE] table={}, id={}, affectedRows={}", mapping.sourceTable(), id, updated);
	}

	private void deletePost(RowPayload payload, LoaderTableMapping mapping) {
		Long id = keyValue(payload, mapping);
		Timer.Sample sample = Timer.start(meterRegistry);
		int deleted = jdbcTemplate.update("DELETE FROM p_post WHERE id = ?", id);
		sample.stop(Timer.builder(LoaderMetrics.Names.MYSQL_DELETE_DURATION)
				.description("MySQL delete duration")
				.tag(LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE)
				.tag(LoaderMetrics.Tags.TABLE, mapping.sourceTable())
				.register(meterRegistry));
		log.info("[LOAD][DELETE] table={}, id={}, affectedRows={}", mapping.sourceTable(), id, deleted);
	}

	// 부모 사용자 존재 여부를 확인해 없는 자식 row 적재 차단
	private void validateParentExists(RowPayload payload, LoaderTableMapping mapping) {
		Map<String, Object> data = payload.data();
		if (data == null || !hasText(data.get(USER_ID))) {
			return;
		}
		Long userId = asLong(data.get(USER_ID));
		if (!parentExists(userId)) {
			throw nonRetryable(
					FailureType.PARENT_ROW_NOT_FOUND,
					"부모 P_USERS row가 없어 P_POST 적재를 보류해야 합니다",
					Map.of(
							"table", mapping.sourceTable(),
							"operation", payload.operation(),
							"userId", String.valueOf(userId)));
		}
		logParentState(mapping, userId, true);
	}

	// 부모 사용자 존재 여부를 로그로 남기는 임시 관찰
	private void logParentState(RowPayload payload, LoaderTableMapping mapping) {
		Map<String, Object> data = payload.data();
		if (data == null || !hasText(data.get(USER_ID))) {
			return;
		}
		Long userId = asLong(data.get(USER_ID));
		logParentState(mapping, userId, parentExists(userId));
	}

	private boolean parentExists(Long userId) {
		Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM p_users WHERE id = ?", Integer.class, userId);
		return count != null && count > 0;
	}

	private void logParentState(LoaderTableMapping mapping, Long userId, boolean exists) {
		log.info("[LOAD][PARENT][CHECK] table={}, parentTable=P_USERS, userId={}, exists={}",
				mapping.sourceTable(),
				userId,
				exists);
	}

	// 변경 컬럼만 SET 절에 포함해 불필요한 null 덮어쓰기 방지
	private UpdateStatement updateStatement(Map<String, Object> data, LoaderTableMapping mapping, Long id) {
		if (data == null || data.isEmpty()) {
			throw nonRetryable(
					FailureType.MISSING_REQUIRED_FIELD,
					"update payload에 변경 컬럼이 없습니다",
					Map.of("table", mapping.sourceTable(), "operation", "UPDATE"));
		}

		List<String> assignments = new ArrayList<>();
		List<Object> params = new ArrayList<>();
		for (String column : UPDATE_COLUMNS) {
			if (data.containsKey(column)) {
				assignments.add(mysqlColumn(column) + " = ?");
				params.add(mysqlValue(column, data.get(column)));
			}
		}
		if (assignments.isEmpty()) {
			throw nonRetryable(
					FailureType.MISSING_REQUIRED_FIELD,
					"update payload에 적재 가능한 변경 컬럼이 없습니다",
					Map.of("table", mapping.sourceTable(), "operation", "UPDATE", "columns", data.keySet().toString()));
		}
		params.add(id);
		return new UpdateStatement("UPDATE p_post SET %s WHERE id = ?".formatted(String.join(", ", assignments)), params);
	}

	private String mysqlColumn(String sourceColumn) {
		return switch (sourceColumn) {
			case USER_ID -> "user_id";
			case TITLE -> "title";
			case CONTENT -> "content";
			case STATUS -> "status";
			case CREATED_AT -> "created_at";
			case UPDATED_AT -> "updated_at";
			default -> throw nonRetryable(
					FailureType.MISSING_REQUIRED_FIELD,
					"지원하지 않는 update 컬럼입니다",
					Map.of("column", sourceColumn));
		};
	}

	private Object mysqlValue(String sourceColumn, Object value) {
		if (CREATED_AT.equals(sourceColumn) || UPDATED_AT.equals(sourceColumn)) {
			return asLocalDateTime(value);
		}
		if (USER_ID.equals(sourceColumn)) {
			return asLong(value);
		}
		return asString(value);
	}

	private boolean hasRequiredInsertColumns(Map<String, Object> data, LoaderTableMapping mapping) {
		return data != null && mapping.requiredInsertColumns().stream()
				.allMatch(column -> hasText(data.get(column)));
	}

	private boolean hasText(Object value) {
		return value != null && !String.valueOf(value).isBlank();
	}

	private Long asLong(Object value) {
		if (value instanceof Number number) {
			return number.longValue();
		}
		try {
			return Long.parseLong(String.valueOf(value));
		}
		catch (NumberFormatException ex) {
			throw nonRetryable(
					FailureType.TYPE_CONVERSION_FAILED,
					"숫자 형식으로 변환할 수 없습니다",
					Map.of("value", String.valueOf(value)));
		}
	}

	private String asString(Object value) {
		return value == null ? null : String.valueOf(value);
	}

	private LocalDateTime asLocalDateTime(Object value) {
		if (value instanceof LocalDateTime localDateTime) {
			return localDateTime;
		}
		if (value instanceof Timestamp timestamp) {
			return timestamp.toLocalDateTime();
		}
		String stringValue = String.valueOf(value);
		for (DateTimeFormatter formatter : dateTimeFormatters()) {
			try {
				return LocalDateTime.parse(stringValue, formatter);
			}
			catch (DateTimeParseException ignored) {
				// 다음 날짜 포맷 시도
			}
		}
		throw nonRetryable(
				FailureType.TIMESTAMP_PARSE_FAILED,
				"timestamp 형식을 변환할 수 없습니다",
				Map.of("value", stringValue));
	}

	private List<DateTimeFormatter> dateTimeFormatters() {
		return List.of(
				DateTimeFormatter.ISO_LOCAL_DATE_TIME,
				DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.n]"),
				ORACLE_LOGMINER_TIMESTAMP_FORMATTER);
	}

	private Long keyValue(RowPayload payload, LoaderTableMapping mapping) {
		validateKey(payload, mapping);
		return asLong(payload.key().get(mapping.keyColumn()));
	}

	private void validateKey(RowPayload payload, LoaderTableMapping mapping) {
		if (payload.key() == null || !hasText(payload.key().get(mapping.keyColumn()))) {
			throw nonRetryable(
					FailureType.MISSING_KEY,
					"payload key에 기준 컬럼이 없습니다",
					context(payload, mapping));
		}
	}

	private void validateRequiredInsertColumns(RowPayload payload, LoaderTableMapping mapping) {
		if (!hasRequiredInsertColumns(payload.data(), mapping)) {
			throw nonRetryable(
					FailureType.MISSING_REQUIRED_FIELD,
					"insert payload에 필수 컬럼이 없습니다",
					context(payload, mapping));
		}
	}

	private Map<String, String> context(RowPayload payload, LoaderTableMapping mapping) {
		return Map.of(
				"table", mapping.sourceTable(),
				"operation", payload.operation() == null ? "UNKNOWN" : payload.operation(),
				"keyColumn", mapping.keyColumn(),
				"columns", payload.data() == null ? "UNKNOWN" : payload.data().keySet().toString());
	}

	private LoadNonRetryableException nonRetryable(FailureType failureType, String reason, Map<String, String> context) {
		return new LoadNonRetryableException(failureType, reason, context);
	}

	private record UpdateStatement(
			String sql,
			List<Object> params) {
	}

}
