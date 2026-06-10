package com.practice.datagenerator.batch.reader;

import java.time.Duration;
import java.time.LocalDateTime;

import org.springframework.batch.infrastructure.item.ItemReader;

import com.practice.datagenerator.observability.GeneratorMetrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public class UserGenerateItemReader implements ItemReader<Integer> {

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
			Timer.builder(GeneratorMetrics.Names.USER_READER_DURATION)
					.description("User generate item reader duration")
					.tag(GeneratorMetrics.Tags.MODULE, GeneratorMetrics.MODULE)
					.register(meterRegistry)
					.record(Duration.between(startTime, endTime));
			meterRegistry.counter(
					GeneratorMetrics.Names.USER_READER_COUNT,
					GeneratorMetrics.Tags.MODULE, GeneratorMetrics.MODULE).increment(current);
			return null;
		}
		current++;
		return current;
	}

}
