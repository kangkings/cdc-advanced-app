package com.practice.logscanner.batch.listener;

import java.time.Duration;
import java.time.LocalDateTime;

import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.stereotype.Component;

import com.practice.logscanner.observability.LogScannerMetrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class RedoLogPrintJobLoggingListener implements JobExecutionListener {

	private final MeterRegistry meterRegistry;

	@Override
	public void beforeJob(JobExecution jobExecution) {
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		Duration elapsed = elapsed(jobExecution.getStartTime(), jobExecution.getEndTime());
		long writeCount = jobExecution.getStepExecutions().stream()
				.mapToLong(StepExecution::getWriteCount).sum();

		Timer.builder(LogScannerMetrics.Names.REDO_LOG_JOB_DURATION)
				.description("Redo log print batch job duration")
				.tag(LogScannerMetrics.Tags.MODULE, LogScannerMetrics.MODULE)
				.tag(LogScannerMetrics.Tags.JOB, jobExecution.getJobInstance().getJobName())
				.tag(LogScannerMetrics.Tags.STATUS, jobExecution.getStatus().name())
				.register(meterRegistry)
				.record(elapsed);

		if (jobExecution.getStatus().isUnsuccessful()) {
			meterRegistry.counter(
					LogScannerMetrics.Names.REDO_LOG_JOB_FAILURE_COUNT,
					LogScannerMetrics.Tags.MODULE, LogScannerMetrics.MODULE,
					LogScannerMetrics.Tags.JOB, jobExecution.getJobInstance().getJobName(),
					LogScannerMetrics.Tags.STATUS, jobExecution.getStatus().name()).increment();
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
