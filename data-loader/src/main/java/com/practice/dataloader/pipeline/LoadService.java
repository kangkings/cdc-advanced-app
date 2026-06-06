package com.practice.dataloader.pipeline;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.Map;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.practice.dataloader.model.RowPayload;
import com.practice.dataloader.model.TransformEvent;
import com.practice.dataloader.mysql.User;
import com.practice.dataloader.mysql.UserRepository;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class LoadService {

	private static final String MODULE = "data-loader";
	private static final String LOAD_DURATION = "data_loader.mysql.load.duration";
	private static final String LOAD_COUNT = "data_loader.mysql.load.count";
	private static final DateTimeFormatter ORACLE_LOGMINER_TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
			.appendPattern("yy/MM/dd HH:mm:ss")
			.optionalStart()
			.appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
			.optionalEnd()
			.toFormatter();

	private final UserRepository userRepository;
	private final MeterRegistry meterRegistry;

	@Transactional
	public void load(TransformEvent event) {
		Timer.Sample sample = Timer.start(meterRegistry);
		String status = "SUCCESS";
		String table = tableName(event);
		String operation = operation(event);

		try {
			if (shouldSkip(event)) {
				status = "SKIPPED";
				log.debug("[LOAD][SKIP] table={}, operation={}, reason={}", table, operation, event.check().reason());
				return;
			}

			RowPayload payload = event.payload();
			if (!"P_USERS".equalsIgnoreCase(payload.tableName())) {
				status = "SKIPPED";
				log.debug("[LOAD][SKIP] unsupported table={}", payload.tableName());
				return;
			}

			switch (payload.operation().toUpperCase(Locale.ROOT)) {
				case "INSERT" -> insertUser(payload);
				case "UPDATE" -> updateUser(payload);
				case "DELETE" -> deleteUser(payload);
				default -> {
					status = "SKIPPED";
					log.debug("[LOAD][SKIP] unsupported operation={}", payload.operation());
				}
			}
		}
		catch (Exception ex) {
			status = "FAILED";
			throw ex;
		}
		finally {
			meterRegistry.counter(LOAD_COUNT, "module", MODULE, "table", table, "operation", operation, "status", status)
					.increment();
			sample.stop(Timer.builder(LOAD_DURATION)
					.description("MySQL load duration")
					.tag("module", MODULE)
					.tag("table", table)
					.tag("operation", operation)
					.tag("status", status)
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

		User user = userRepository.findById(id).orElseGet(() -> new User(id));
		applyIfPresent(data, "NAME", value -> user.setName(asString(value)));
		applyIfPresent(data, "EMAIL", value -> user.setEmail(asString(value)));
		applyIfPresent(data, "STATUS", value -> user.setStatus(asString(value)));
		applyIfPresent(data, "CREATED_AT", value -> user.setCreatedAt(asLocalDateTime(value)));
		applyIfPresent(data, "UPDATED_AT", value -> user.setUpdatedAt(asLocalDateTime(value)));

		userRepository.save(user);
		log.info("[LOAD][INSERT] table=P_USERS, id={}", id);
	}

	private void updateUser(RowPayload payload) {
		Long id = asLong(payload.key().get("ID"));
		User user = userRepository.findById(id)
				.orElse(null);
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

		userRepository.save(user);
		log.info("[LOAD][UPDATE] table=P_USERS, id={}", id);
	}

	private void deleteUser(RowPayload payload) {
		Long id = asLong(payload.key().get("ID"));
		userRepository.deleteById(id);
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
				// try next format
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
