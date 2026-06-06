package com.practice.datagenerator.batch.listener;

import java.time.Duration;
import java.time.LocalDateTime;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserGenerateStepLoggingListener implements StepExecutionListener {

	private static final String MODULE = "data-generator";
	private static final String STEP_DURATION = "data_generator.user.step.duration";
	private static final String READ_COUNT = "data_generator.user.read.count";
	private static final String WRITE_COUNT = "data_generator.user.write.count";
	private static final String FILTER_COUNT = "data_generator.user.filter.count";
	private static final String SKIP_COUNT = "data_generator.user.skip.count";

	private final MeterRegistry meterRegistry;

	@Override
	public void beforeStep(StepExecution stepExecution) {
		log.info("[USER-GENERATE][STEP][START] stepName={}, stepExecutionId={}",
				stepExecution.getStepName(),
				stepExecution.getId());
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		LocalDateTime startTime = stepExecution.getStartTime();
		LocalDateTime endTime = stepExecution.getEndTime();
		Duration elapsed = elapsed(startTime, endTime);
		String status = stepExecution.getStatus().name();
		String stepName = stepExecution.getStepName();

		Timer.builder(STEP_DURATION)
				.description("User generate batch step duration")
				.tag("module", MODULE)
				.tag("step", stepName)
				.tag("status", status)
				.register(meterRegistry)
				.record(elapsed);

		increment(READ_COUNT, stepName, status, stepExecution.getReadCount());
		increment(WRITE_COUNT, stepName, status, stepExecution.getWriteCount());
		increment(FILTER_COUNT, stepName, status, stepExecution.getFilterCount());
		increment(SKIP_COUNT, stepName, status,
				stepExecution.getReadSkipCount() + stepExecution.getProcessSkipCount() + stepExecution.getWriteSkipCount());

		log.info("[USER-GENERATE][STEP][END] stepName={}, stepExecutionId={}, status={}, readCount={}, writeCount={}, elapsedMs={}",
				stepExecution.getStepName(),
				stepExecution.getId(),
				stepExecution.getStatus(),
				stepExecution.getReadCount(),
				stepExecution.getWriteCount(),
				elapsed.toMillis());

		return stepExecution.getExitStatus();
	}

	private void increment(String name, String stepName, String status, long amount) {
		if (amount <= 0) {
			return;
		}
		meterRegistry.counter(name,
				"module", MODULE,
				"step", stepName,
				"status", status).increment(amount);
	}

	private Duration elapsed(LocalDateTime startTime, LocalDateTime endTime) {
		if (startTime == null || endTime == null) {
			return Duration.ZERO;
		}
		return Duration.between(startTime, endTime);
	}

}
