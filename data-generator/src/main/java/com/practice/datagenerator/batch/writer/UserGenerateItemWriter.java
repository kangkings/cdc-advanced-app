package com.practice.datagenerator.batch.writer;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;

import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.practice.datagenerator.domain.user.domain.entity.User;
import com.practice.datagenerator.observability.GeneratorMetrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class UserGenerateItemWriter implements ItemWriter<User> {

	private final JdbcTemplate jdbcTemplate;
	private final MeterRegistry meterRegistry;

	@Override
	public void write(Chunk<? extends User> chunk) {
		LocalDateTime startTime = LocalDateTime.now();

		jdbcTemplate.batchUpdate("""
				INSERT INTO p_users (
					name,
					email,
					status,
					created_at,
					updated_at
				)
				VALUES (?, ?, ?, ?, ?)
				""",
				chunk.getItems(),
				chunk.size(),
				(statement, user) -> {
					statement.setString(1, user.name());
					statement.setString(2, user.email());
					statement.setString(3, user.status().name());
					statement.setTimestamp(4, Timestamp.valueOf(user.createdAt()));
					statement.setTimestamp(5, Timestamp.valueOf(user.updatedAt()));
				});

		LocalDateTime endTime = LocalDateTime.now();
		Timer.builder(GeneratorMetrics.Names.USER_WRITER_DURATION)
				.description("User generate item writer duration")
				.tag(GeneratorMetrics.Tags.MODULE, GeneratorMetrics.MODULE)
				.register(meterRegistry)
				.record(Duration.between(startTime, endTime));
		meterRegistry.counter(
				GeneratorMetrics.Names.USER_WRITER_CHUNK_COUNT,
				GeneratorMetrics.Tags.MODULE, GeneratorMetrics.MODULE).increment();
		meterRegistry.counter(
				GeneratorMetrics.Names.USER_WRITER_ITEM_COUNT,
				GeneratorMetrics.Tags.MODULE, GeneratorMetrics.MODULE).increment(chunk.size());
	}

}
