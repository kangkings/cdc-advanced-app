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
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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
@Service
@RequiredArgsConstructor
public class LoadService {

	private static final DateTimeFormatter ORACLE_LOGMINER_TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
			.appendPattern("yy/MM/dd HH:mm:ss")
			.optionalStart()
			.appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
			.optionalEnd()
			.toFormatter();

	private final JdbcTemplate jdbcTemplate;
	private final UserRepository userRepository;
	private final MeterRegistry meterRegistry;

	@Transactional
	public void loadBatch(List<TransformEvent> events) {
		LocalDateTime startTime = LocalDateTime.now();

		List<TransformEvent> insertEvents = events.stream()
				.filter(e -> !shouldSkip(e))
				.filter(e -> LoaderMetrics.P_USERS.equalsIgnoreCase(e.payload().tableName()))
				.filter(e -> "INSERT".equalsIgnoreCase(e.payload().operation()))
				.filter(e -> hasRequiredInsertColumns(e.payload().data()))
				.toList();

		List<TransformEvent> otherEvents = events.stream()
				.filter(e -> !shouldSkip(e))
				.filter(e -> LoaderMetrics.P_USERS.equalsIgnoreCase(e.payload().tableName()))
				.filter(e -> !"INSERT".equalsIgnoreCase(e.payload().operation()))
				.toList();

		if (!insertEvents.isEmpty()) {
			jdbcTemplate.batchUpdate("""
					INSERT INTO p_users (id, name, email, status, created_at, updated_at)
					VALUES (?, ?, ?, ?, ?, ?)
					""",
					insertEvents,
					insertEvents.size(),
					(ps, event) -> {
						Map<String, Object> data = event.payload().data();
						ps.setLong(1, asLong(event.payload().key().get("ID")));
						ps.setString(2, asString(data.get("NAME")));
						ps.setString(3, asString(data.get("EMAIL")));
						ps.setString(4, asString(data.get("STATUS")));
						ps.setObject(5, asLocalDateTime(data.get("CREATED_AT")));
						ps.setObject(6, asLocalDateTime(data.get("UPDATED_AT")));
					});

			Duration elapsed = Duration.between(startTime, LocalDateTime.now());
			Timer.builder(LoaderMetrics.Names.MYSQL_BATCH_INSERT_DURATION)
					.description("MySQL batch insert duration")
					.tag(LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE)
					.tag(LoaderMetrics.Tags.TABLE, LoaderMetrics.P_USERS)
					.register(meterRegistry)
					.record(elapsed);
			meterRegistry.counter(
					LoaderMetrics.Names.MYSQL_BATCH_INSERT_COUNT,
					LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE,
					LoaderMetrics.Tags.TABLE, LoaderMetrics.P_USERS)
					.increment(insertEvents.size());
		}

		for (TransformEvent event : otherEvents) {
			load(event);
		}
	}

	@Transactional
	public void load(TransformEvent event) {
		Timer.Sample sample = Timer.start(meterRegistry);
		String status = LoaderMetrics.Status.SUCCESS;
		String table = tableName(event);
		String operation = operation(event);

		try {
			if (shouldSkip(event)) {
				status = LoaderMetrics.Status.SKIPPED;
				log.debug("[LOAD][SKIP] table={}, operation={}, reason={}", table, operation, event.check().reason());
				return;
			}

			RowPayload payload = event.payload();
			if (!LoaderMetrics.P_USERS.equalsIgnoreCase(payload.tableName())) {
				status = LoaderMetrics.Status.SKIPPED;
				log.debug("[LOAD][SKIP] unsupported table={}", payload.tableName());
				return;
			}

			switch (payload.operation().toUpperCase(Locale.ROOT)) {
				case "INSERT" -> insertUser(payload);
				case "UPDATE" -> updateUser(payload);
				case "DELETE" -> deleteUser(payload);
				default -> {
					status = LoaderMetrics.Status.SKIPPED;
					log.debug("[LOAD][SKIP] unsupported operation={}", payload.operation());
				}
			}
		}
		catch (Exception ex) {
			status = LoaderMetrics.Status.FAILED;
			throw ex;
		}
		finally {
			meterRegistry.counter(
					LoaderMetrics.Names.MYSQL_LOAD_COUNT,
					LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE,
					LoaderMetrics.Tags.TABLE, table,
					LoaderMetrics.Tags.OPERATION, operation,
					LoaderMetrics.Tags.STATUS, status)
					.increment();
			sample.stop(Timer.builder(LoaderMetrics.Names.MYSQL_LOAD_DURATION)
					.description("MySQL load duration")
					.tag(LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE)
					.tag(LoaderMetrics.Tags.TABLE, table)
					.tag(LoaderMetrics.Tags.OPERATION, operation)
					.tag(LoaderMetrics.Tags.STATUS, status)
					.register(meterRegistry));
		}
	}

	private boolean shouldSkip(TransformEvent event) {
		return event == null
				|| event.check() == null
				|| !event.check().valid()
				|| !event.check().supported()
				|| event.payload() == null;
	}

	private void insertUser(RowPayload payload) {
		Long id = asLong(payload.key().get("ID"));
		Map<String, Object> data = payload.data();
		if (!hasRequiredInsertColumns(data)) {
			log.warn("[LOAD][SKIP] insert payload is missing required columns. table=P_USERS, id={}, columns={}",
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
				asString(data.get("NAME")),
				asString(data.get("EMAIL")),
				asString(data.get("STATUS")),
				asLocalDateTime(data.get("CREATED_AT")),
				asLocalDateTime(data.get("UPDATED_AT")));

		sample.stop(Timer.builder(LoaderMetrics.Names.MYSQL_INSERT_DURATION)
				.description("MySQL direct insert duration")
				.tag(LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE)
				.tag(LoaderMetrics.Tags.TABLE, LoaderMetrics.P_USERS)
				.register(meterRegistry));
		meterRegistry.counter(
				LoaderMetrics.Names.MYSQL_INSERT_COUNT,
				LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE,
				LoaderMetrics.Tags.TABLE, LoaderMetrics.P_USERS,
				LoaderMetrics.Tags.STATUS, LoaderMetrics.Status.SUCCESS).increment();
		log.info("[LOAD][INSERT] table=P_USERS, id={}", id);
	}

	private void updateUser(RowPayload payload) {
		Long id = asLong(payload.key().get("ID"));
		Timer.Sample findSample = Timer.start(meterRegistry);
		User user = userRepository.findById(id)
				.orElse(null);
		findSample.stop(Timer.builder(LoaderMetrics.Names.MYSQL_UPDATE_FIND_DURATION)
				.description("MySQL update path find duration")
				.tag(LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE)
				.tag(LoaderMetrics.Tags.TABLE, LoaderMetrics.P_USERS)
				.register(meterRegistry));
		if (user == null) {
			log.warn("[LOAD][SKIP] update target does not exist. table=P_USERS, id={}, columns={}",
					id,
					payload.data() == null ? null : payload.data().keySet());
			return;
		}

		Map<String, Object> data = payload.data();
		applyIfPresent(data, "NAME", value -> user.setName(asString(value)));
		applyIfPresent(data, "EMAIL", value -> user.setEmail(asString(value)));
		applyIfPresent(data, "STATUS", value -> user.setStatus(asString(value)));
		applyIfPresent(data, "CREATED_AT", value -> user.setCreatedAt(asLocalDateTime(value)));
		applyIfPresent(data, "UPDATED_AT", value -> user.setUpdatedAt(asLocalDateTime(value)));

		Timer.Sample saveSample = Timer.start(meterRegistry);
		userRepository.save(user);
		saveSample.stop(Timer.builder(LoaderMetrics.Names.MYSQL_UPDATE_SAVE_DURATION)
				.description("MySQL update path save duration")
				.tag(LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE)
				.tag(LoaderMetrics.Tags.TABLE, LoaderMetrics.P_USERS)
				.register(meterRegistry));
		log.info("[LOAD][UPDATE] table=P_USERS, id={}", id);
	}

	private void deleteUser(RowPayload payload) {
		Long id = asLong(payload.key().get("ID"));
		Timer.Sample deleteSample = Timer.start(meterRegistry);
		userRepository.deleteById(id);
		deleteSample.stop(Timer.builder(LoaderMetrics.Names.MYSQL_DELETE_DURATION)
				.description("MySQL delete duration")
				.tag(LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE)
				.tag(LoaderMetrics.Tags.TABLE, LoaderMetrics.P_USERS)
				.register(meterRegistry));
		log.info("[LOAD][DELETE] table=P_USERS, id={}", id);
	}

	private void applyIfPresent(Map<String, Object> data, String key, ValueApplier applier) {
		if (data != null && data.containsKey(key)) {
			applier.apply(data.get(key));
		}
	}

	private boolean hasRequiredInsertColumns(Map<String, Object> data) {
		return data != null
				&& hasText(data.get("NAME"))
				&& hasText(data.get("EMAIL"))
				&& hasText(data.get("STATUS"))
				&& data.get("CREATED_AT") != null
				&& data.get("UPDATED_AT") != null;
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

	private java.util.List<DateTimeFormatter> dateTimeFormatters() {
		return java.util.List.of(
				DateTimeFormatter.ISO_LOCAL_DATE_TIME,
				DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.n]"),
				ORACLE_LOGMINER_TIMESTAMP_FORMATTER);
	}

	private String tableName(TransformEvent event) {
		if (event == null || event.payload() == null || event.payload().tableName() == null) {
			return "UNKNOWN";
		}
		return event.payload().tableName();
	}

	private String operation(TransformEvent event) {
		if (event == null || event.payload() == null || event.payload().operation() == null) {
			return "UNKNOWN";
		}
		return event.payload().operation();
	}

	@FunctionalInterface
	private interface ValueApplier {
		void apply(Object value);
	}

}
