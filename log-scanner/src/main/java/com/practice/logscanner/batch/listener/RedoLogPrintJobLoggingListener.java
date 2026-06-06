package com.practice.logscanner.batch.listener;

import java.time.Duration;
import java.time.LocalDateTime;

import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class RedoLogPrintJobLoggingListener implements JobExecutionListener {

	private static final String MODULE = "log-scanner";
	private static final String JOB_DURATION = "log_scanner.redo_log.job.duration";
	private static final String JOB_FAILURE_COUNT = "log_scanner.redo_log.job.failure.count";

	private final MeterRegistry meterRegistry;

	@Override
	public void beforeJob(JobExecution jobExecution) {
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		Duration elapsed = elapsed(jobExecution.getStartTime(), jobExecution.getEndTime());
		long writeCount = jobExecution.getStepExecutions().stream()
				.mapToLong(StepExecution::getWriteCount).sum();

		Timer.builder(JOB_DURATION)
				.description("Redo log print batch job duration")
				.tag("module", MODULE)
				.tag("job", jobExecution.getJobInstance().getJobName())
				.tag("status", jobExecution.getStatus().name())
				.register(meterRegistry)
				.record(elapsed);

		if (jobExecution.getStatus().isUnsuccessful()) {
			meterRegistry.counter(JOB_FAILURE_COUNT,
					"module", MODULE,
					"job", jobExecution.getJobInstance().getJobName(),
					"status", jobExecution.getStatus().name()).increment();
			log.error("[REDO-LOG-PRINT][JOB][END] status={}, elapsedMs={}", jobExecution.getStatus(), elapsed.toMillis());
			return;
		}

		if (writeCount > 0) {
			log.info("[REDO-LOG-PRINT][JOB][END] status={}, writeCount={}, elapsedMs={}",
					jobExecution.getStatus(), writeCount, elapsed.toMillis());
		}
	}

	private Duration elapsed(LocalDateTime startTime, LocalDateTime endTime) {
		if (startTime == null || endTime == null) {
			return Duration.ZERO;
		}
		return Duration.between(startTime, endTime);
	}

}
