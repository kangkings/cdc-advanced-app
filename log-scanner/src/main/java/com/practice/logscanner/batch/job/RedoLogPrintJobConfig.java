package com.practice.logscanner.batch.job;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.practice.logscanner.batch.listener.RedoLogPrintJobLoggingListener;

@Configuration
public class RedoLogPrintJobConfig {

	@Bean
	public Job redoLogPrintJob(
			JobRepository jobRepository,
			Step redoLogPrintStep,
			RedoLogPrintJobLoggingListener redoLogPrintJobLoggingListener) {
		return new JobBuilder("redoLogPrintJob", jobRepository)
				.listener(redoLogPrintJobLoggingListener)
				.start(redoLogPrintStep)
				.build();
	}

}
