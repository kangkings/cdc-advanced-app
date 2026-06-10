package com.practice.datagenerator.batch.listener;

import java.time.Duration;
import java.time.LocalDateTime;

import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.stereotype.Component;

import com.practice.datagenerator.observability.GeneratorMetrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserGenerateJobLoggingListener implements JobExecutionListener {

	private final MeterRegistry meterRegistry;

	@Override
	public void beforeJob(JobExecution jobExecution) {
		log.info("[USER-GENERATE][JOB][START] jobName={}, jobExecutionId={}, parameters={}",
				jobExecution.getJobInstance().getJobName(),
				jobExecution.getId(),
				jobExecution.getJobParameters());
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		LocalDateTime startTime = jobExecution.getStartTime();
		LocalDateTime endTime = jobExecution.getEndTime();
		Duration elapsed = elapsed(startTime, endTime);

		Timer.builder(GeneratorMetrics.Names.USER_JOB_DURATION)
				.description("User generate batch job duration")
				.tag(GeneratorMetrics.Tags.MODULE, GeneratorMetrics.MODULE)
				.tag(GeneratorMetrics.Tags.JOB, jobExecution.getJobInstance().getJobName())
				.tag(GeneratorMetrics.Tags.STATUS, jobExecution.getStatus().name())
				.register(meterRegistry)
				.record(elapsed);

		if (jobExecution.getStatus().isUnsuccessful()) {
			meterRegistry.counter(
					GeneratorMetrics.Names.USER_JOB_FAILURE_COUNT,
					GeneratorMetrics.Tags.MODULE, GeneratorMetrics.MODULE,
					GeneratorMetrics.Tags.JOB, jobExecution.getJobInstance().getJobName(),
					GeneratorMetrics.Tags.STATUS, jobExecution.getStatus().name()).increment();
		}

		log.info("[USER-GENERATE][JOB][END] jobName={}, jobExecutionId={}, status={}, elapsedMs={}",
				jobExecution.getJobInstance().getJobName(),
				jobExecution.getId(),
				jobExecution.getStatus(),
				elapsed.toMillis());
	}

	private Duration elapsed(LocalDateTime startTime, LocalDateTime endTime) {
		if (startTime == null || endTime == null) {
			return Duration.ZERO;
		}
		return Duration.between(startTime, endTime);
	}

}
