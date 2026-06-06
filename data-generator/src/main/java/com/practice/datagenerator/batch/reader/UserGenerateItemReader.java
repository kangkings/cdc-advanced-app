package com.practice.datagenerator.batch.reader;

import java.time.Duration;
import java.time.LocalDateTime;

import org.springframework.batch.infrastructure.item.ItemReader;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public class UserGenerateItemReader implements ItemReader<Integer> {

	private static final String MODULE = "data-generator";
	private static final String READER_DURATION = "data_generator.user.reader.duration";
	private static final String READER_COUNT = "data_generator.user.reader.count";

	private final int count;
	private final MeterRegistry meterRegistry;
	private int current;
	private LocalDateTime startTime;

	public UserGenerateItemReader(int count, MeterRegistry meterRegistry) {
		this.count = count;
		this.meterRegistry = meterRegistry;
	}

	@Override
	public Integer read() {
		if (startTime == null) {
			startTime = LocalDateTime.now();
		}

		if (current >= count) {
			LocalDateTime endTime = LocalDateTime.now();
			Timer.builder(READER_DURATION)
					.description("User generate item reader duration")
					.tag("module", MODULE)
					.register(meterRegistry)
					.record(Duration.between(startTime, endTime));
			meterRegistry.counter(READER_COUNT, "module", MODULE).increment(current);
			return null;
		}
		current++;
		return current;
	}

}
