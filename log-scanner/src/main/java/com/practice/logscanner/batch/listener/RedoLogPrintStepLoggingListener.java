package com.practice.logscanner.batch.listener;

import java.time.Duration;
import java.time.LocalDateTime;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.listener.StepExecutionListener;
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
public class RedoLogPrintStepLoggingListener implements StepExecutionListener {

	private final MeterRegistry meterRegistry;

	@Override
	public void beforeStep(StepExecution stepExecution) {
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		Duration elapsed = elapsed(stepExecution.getStartTime(), stepExecution.getEndTime());
		String status = stepExecution.getStatus().name();
		String stepName = stepExecution.getStepName();

		Timer.builder(LogScannerMetrics.Names.REDO_LOG_STEP_DURATION)
				.description("Redo log print batch step duration")
				.tag(LogScannerMetrics.Tags.MODULE, LogScannerMetrics.MODULE)
				.tag(LogScannerMetrics.Tags.STEP, stepName)
				.tag(LogScannerMetrics.Tags.STATUS, status)
				.register(meterRegistry)
				.record(elapsed);

		increment(LogScannerMetrics.Names.REDO_LOG_READ_COUNT, stepName, status, stepExecution.getReadCount());
		increment(LogScannerMetrics.Names.REDO_LOG_WRITE_COUNT, stepName, status, stepExecution.getWriteCount());
		increment(LogScannerMetrics.Names.REDO_LOG_SKIP_COUNT, stepName, status,
				stepExecution.getReadSkipCount() + stepExecution.getProcessSkipCount() + stepExecution.getWriteSkipCount());

		if (stepExecution.getWriteCount() > 0) {
			log.info("[REDO-LOG-PRINT][STEP][END] stepName={}, status={}, readCount={}, writeCount={}, elapsedMs={}",
					stepName, status,
					stepExecution.getReadCount(),
					stepExecution.getWriteCount(),
					elapsed.toMillis());
		}

		return stepExecution.getExitStatus();
	}

	private void increment(String name, String stepName, String status, long amount) {
		if (amount <= 0) {
			return;
		}
		meterRegistry.counter(name,
				LogScannerMetrics.Tags.MODULE, LogScannerMetrics.MODULE,
				LogScannerMetrics.Tags.STEP, stepName,
				LogScannerMetrics.Tags.STATUS, status).increment(amount);
	}

	private Duration elapsed(LocalDateTime startTime, LocalDateTime endTime) {
		if (startTime == null || endTime == null) {
			return Duration.ZERO;
		}
		return Duration.between(startTime, endTime);
	}

}
