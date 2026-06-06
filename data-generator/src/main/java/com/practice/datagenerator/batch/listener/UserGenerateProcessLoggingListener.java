package com.practice.datagenerator.batch.listener;

import org.springframework.batch.core.listener.ItemProcessListener;
import org.springframework.stereotype.Component;

import com.practice.datagenerator.domain.user.domain.entity.User;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserGenerateProcessLoggingListener implements ItemProcessListener<Integer, User> {

	private static final String MODULE = "data-generator";
	private static final String PROCESS_DURATION = "data_generator.user.processor.duration";
	private static final String PROCESS_COUNT = "data_generator.user.process.count";
	private static final String PROCESS_ERROR_COUNT = "data_generator.user.process.error.count";

	private final MeterRegistry meterRegistry;
	private final ThreadLocal<Timer.Sample> sampleHolder = new ThreadLocal<>();

	@Override
	public void beforeProcess(Integer item) {
		sampleHolder.set(Timer.start(meterRegistry));
	}

	@Override
	public void afterProcess(Integer item, User result) {
		stopTimer("SUCCESS");
		meterRegistry.counter(PROCESS_COUNT, "module", MODULE, "status", "SUCCESS").increment();
	}

	@Override
	public void onProcessError(Integer item, Exception exception) {
		stopTimer("FAILED");
		meterRegistry.counter(PROCESS_ERROR_COUNT, "module", MODULE).increment();
		log.error("[USER-GENERATE][PROCESSOR][ERROR] item={}", item, exception);
	}

	private void stopTimer(String status) {
		Timer.Sample sample = sampleHolder.get();
		if (sample == null) {
			return;
		}
		sample.stop(Timer.builder(PROCESS_DURATION)
				.description("User generate item processor duration")
				.tag("module", MODULE)
				.tag("status", status)
				.register(meterRegistry));
		sampleHolder.remove();
	}

}
