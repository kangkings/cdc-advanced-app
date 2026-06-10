package com.practice.datagenerator.batch.listener;

import org.springframework.batch.core.listener.ItemProcessListener;
import org.springframework.stereotype.Component;

import com.practice.datagenerator.domain.user.domain.entity.User;
import com.practice.datagenerator.observability.GeneratorMetrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserGenerateProcessLoggingListener implements ItemProcessListener<Integer, User> {

	private final MeterRegistry meterRegistry;
	private final ThreadLocal<Timer.Sample> sampleHolder = new ThreadLocal<>();

	@Override
	public void beforeProcess(Integer item) {
		sampleHolder.set(Timer.start(meterRegistry));
	}

	@Override
	public void afterProcess(Integer item, User result) {
		stopTimer(GeneratorMetrics.Status.SUCCESS);
		meterRegistry.counter(
				GeneratorMetrics.Names.USER_PROCESS_COUNT,
				GeneratorMetrics.Tags.MODULE, GeneratorMetrics.MODULE,
				GeneratorMetrics.Tags.STATUS, GeneratorMetrics.Status.SUCCESS).increment();
	}

	@Override
	public void onProcessError(Integer item, Exception exception) {
		stopTimer(GeneratorMetrics.Status.FAILED);
		meterRegistry.counter(
				GeneratorMetrics.Names.USER_PROCESS_ERROR_COUNT,
				GeneratorMetrics.Tags.MODULE, GeneratorMetrics.MODULE).increment();
		log.error("[USER-GENERATE][PROCESSOR][ERROR] item={}", item, exception);
	}

	private void stopTimer(String status) {
		Timer.Sample sample = sampleHolder.get();
		if (sample == null) {
			return;
		}
		sample.stop(Timer.builder(GeneratorMetrics.Names.USER_PROCESSOR_DURATION)
				.description("User generate item processor duration")
				.tag(GeneratorMetrics.Tags.MODULE, GeneratorMetrics.MODULE)
				.tag(GeneratorMetrics.Tags.STATUS, status)
				.register(meterRegistry));
		sampleHolder.remove();
	}

}
