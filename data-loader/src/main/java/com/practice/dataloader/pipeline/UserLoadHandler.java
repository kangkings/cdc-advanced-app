package com.practice.dataloader.pipeline;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.practice.dataloader.model.RowPayload;
import com.practice.dataloader.model.TransformEvent;
import com.practice.dataloader.mysql.User;
import com.practice.dataloader.mysql.UserRepository;
import com.practice.dataloader.observability.LoaderMetrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserLoadHandler implements LoadHandler {

	private static final String TARGET_TABLE = "p_users";
	private static final String NAME = "NAME";
	private static final String EMAIL = "EMAIL";
	private static final String STATUS = "STATUS";
	private static final String CREATED_AT = "CREATED_AT";
	private static final String UPDATED_AT = "UPDATED_AT";
	private static final DateTimeFormatter ORACLE_LOGMINER_TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
			.appendPattern("yy/MM/dd HH:mm:ss")
			.optionalStart()
			.appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
			.optionalEnd()
			.toFormatter();

	private final JdbcTemplate jdbcTemplate;
	private final UserRepository userRepository;
	private final MeterRegistry meterRegistry;

	// 사용자 대상 테이블 매핑 처리 여부 확인
	@Override
	public boolean supports(LoaderTableMapping mapping) {
		return mapping != null && TARGET_TABLE.equalsIgnoreCase(mapping.targetTable());
	}

	// 사용자 처리기가 지원하는 CDC 작업 확인
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

	// 사용자 insert 이벤트를 MySQL 배치 insert로 적재
	@Override
	public void loadBatchInsert(List<TransformEvent> events, LoaderTableMapping mapping) {
		List<TransformEvent> insertEvents = events.stream()
				.filter(event -> hasRequiredInsertColumns(event.payload().data(), mapping))
				.toList();
		if (insertEvents.isEmpty()) {
			return;
		}

		LocalDateTime startTime = LocalDateTime.now();
		jdbcTemplate.batchUpdate("""
				INSERT INTO p_users (id, name, email, status, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?, ?)
				""",
				insertEvents,
				insertEvents.size(),
				(ps, event) -> {
					Map<String, Object> data = event.payload().data();
					ps.setLong(1, asLong(event.payload().key().get(mapping.keyColumn())));
					ps.setString(2, asString(data.get(NAME)));
					ps.setString(3, asString(data.get(EMAIL)));
					ps.setString(4, asString(data.get(STATUS)));
					ps.setObject(5, asLocalDateTime(data.get(CREATED_AT)));
					ps.setObject(6, asLocalDateTime(data.get(UPDATED_AT)));
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

	// 사용자 단건 INSERT/UPDATE/DELETE 분기 처리
	@Override
	public void load(RowPayload payload, LoaderTableMapping mapping) {
		switch (payload.operation().toUpperCase(Locale.ROOT)) {
			case "INSERT" -> insertUser(payload, mapping);
			case "UPDATE" -> updateUser(payload, mapping);
			case "DELETE" -> deleteUser(payload, mapping);
			default -> throw new IllegalArgumentException("Unsupported operation: " + payload.operation());
		}
	}

	private void insertUser(RowPayload payload, LoaderTableMapping mapping) {
		Long id = asLong(payload.key().get(mapping.keyColumn()));
		Map<String, Object> data = payload.data();
		if (!hasRequiredInsertColumns(data, mapping)) {
			log.warn("[LOAD][SKIP] insert payload is missing required columns. table={}, id={}, columns={}",
					mapping.sourceTable(),
					id,
					data == null ? null : data.keySet());
			return;
		}

		Timer.Sample sample = Timer.start(meterRegistry);
		jdbcTemplate.update("""
				INSERT INTO p_users (id, name, email, status, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?, ?)
				""",
				id,
				asString(data.get(NAME)),
				asString(data.get(EMAIL)),
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

	private void updateUser(RowPayload payload, LoaderTableMapping mapping) {
		Long id = asLong(payload.key().get(mapping.keyColumn()));
		Timer.Sample findSample = Timer.start(meterRegistry);
		User user = userRepository.findById(id)
				.orElse(null);
		findSample.stop(Timer.builder(LoaderMetrics.Names.MYSQL_UPDATE_FIND_DURATION)
				.description("MySQL update path find duration")
				.tag(LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE)
				.tag(LoaderMetrics.Tags.TABLE, mapping.sourceTable())
				.register(meterRegistry));
		if (user == null) {
			log.warn("[LOAD][SKIP] update target does not exist. table={}, id={}, columns={}",
					mapping.sourceTable(),
					id,
					payload.data() == null ? null : payload.data().keySet());
			return;
		}

		Map<String, Object> data = payload.data();
		applyIfPresent(data, NAME, value -> user.setName(asString(value)));
		applyIfPresent(data, EMAIL, value -> user.setEmail(asString(value)));
		applyIfPresent(data, STATUS, value -> user.setStatus(asString(value)));
		applyIfPresent(data, CREATED_AT, value -> user.setCreatedAt(asLocalDateTime(value)));
		applyIfPresent(data, UPDATED_AT, value -> user.setUpdatedAt(asLocalDateTime(value)));

		Timer.Sample saveSample = Timer.start(meterRegistry);
		userRepository.save(user);
		saveSample.stop(Timer.builder(LoaderMetrics.Names.MYSQL_UPDATE_SAVE_DURATION)
				.description("MySQL update path save duration")
				.tag(LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE)
				.tag(LoaderMetrics.Tags.TABLE, mapping.sourceTable())
				.register(meterRegistry));
		log.info("[LOAD][UPDATE] table={}, id={}", mapping.sourceTable(), id);
	}

	private void deleteUser(RowPayload payload, LoaderTableMapping mapping) {
		Long id = asLong(payload.key().get(mapping.keyColumn()));
		Timer.Sample deleteSample = Timer.start(meterRegistry);
		userRepository.deleteById(id);
		deleteSample.stop(Timer.builder(LoaderMetrics.Names.MYSQL_DELETE_DURATION)
				.description("MySQL delete duration")
				.tag(LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE)
				.tag(LoaderMetrics.Tags.TABLE, mapping.sourceTable())
				.register(meterRegistry));
		log.info("[LOAD][DELETE] table={}, id={}", mapping.sourceTable(), id);
	}

	private void applyIfPresent(Map<String, Object> data, String key, ValueApplier applier) {
		if (data != null && data.containsKey(key)) {
			applier.apply(data.get(key));
		}
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
		return Long.parseLong(String.valueOf(value));
	}

	private String asString(Object value) {
		return value == null ? null : String.valueOf(value);
	}

	LocalDateTime asLocalDateTime(Object value) {
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
		throw new IllegalArgumentException("Unsupported timestamp format: " + stringValue);
	}

	private List<DateTimeFormatter> dateTimeFormatters() {
		return List.of(
				DateTimeFormatter.ISO_LOCAL_DATE_TIME,
				DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.n]"),
				ORACLE_LOGMINER_TIMESTAMP_FORMATTER);
	}

	@FunctionalInterface
	private interface ValueApplier {
		void apply(Object value);
	}

}
