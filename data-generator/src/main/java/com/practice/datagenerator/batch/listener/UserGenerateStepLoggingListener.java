package com.practice.datagenerator.batch.listener;

import java.time.Duration;
import java.time.LocalDateTime;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.stereotype.Component;

import com.practice.datagenerator.observability.GeneratorMetrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserGenerateStepLoggingListener implements StepExecutionListener {

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

		Timer.builder(GeneratorMetrics.Names.USER_STEP_DURATION)
				.description("User generate batch step duration")
				.tag(GeneratorMetrics.Tags.MODULE, GeneratorMetrics.MODULE)
				.tag(GeneratorMetrics.Tags.STEP, stepName)
				.tag(GeneratorMetrics.Tags.STATUS, status)
				.register(meterRegistry)
				.record(elapsed);

		increment(GeneratorMetrics.Names.USER_READ_COUNT, stepName, status, stepExecution.getReadCount());
		increment(GeneratorMetrics.Names.USER_WRITE_COUNT, stepName, status, stepExecution.getWriteCount());
		increment(GeneratorMetrics.Names.USER_FILTER_COUNT, stepName, status, stepExecution.getFilterCount());
		increment(GeneratorMetrics.Names.USER_SKIP_COUNT, stepName, status,
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
				GeneratorMetrics.Tags.MODULE, GeneratorMetrics.MODULE,
				GeneratorMetrics.Tags.STEP, stepName,
				GeneratorMetrics.Tags.STATUS, status).increment(amount);
	}

	private Duration elapsed(LocalDateTime startTime, LocalDateTime endTime) {
		if (startTime == null || endTime == null) {
			return Duration.ZERO;
		}
		return Duration.between(startTime, endTime);
	}

}
