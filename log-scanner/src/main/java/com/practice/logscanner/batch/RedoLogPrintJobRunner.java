package com.practice.logscanner.batch;

import java.time.Instant;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class RedoLogPrintJobRunner implements ApplicationRunner {

	private final boolean enabled;
	private final JobLauncher jobLauncher;
	private final Job redoLogPrintJob;

	public RedoLogPrintJobRunner(
			@Value("${batch.redo-log-print.enabled:true}") boolean enabled,
			JobLauncher jobLauncher,
			Job redoLogPrintJob) {
		this.enabled = enabled;
		this.jobLauncher = jobLauncher;
		this.redoLogPrintJob = redoLogPrintJob;
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		if (!enabled) {
			log.info("Redo log print batch is disabled.");
			return;
		}

		JobParameters parameters = new JobParametersBuilder()
				.addString("requestedAt", Instant.now().toString())
				.toJobParameters();

		log.info("Starting redo log print batch.");
		jobLauncher.run(redoLogPrintJob, parameters);
	}

}
