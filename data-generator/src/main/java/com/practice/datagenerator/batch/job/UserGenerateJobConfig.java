package com.practice.datagenerator.batch.job;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.practice.datagenerator.batch.listener.UserGenerateJobLoggingListener;

@Configuration
public class UserGenerateJobConfig {

	@Bean
	public Job userGenerateJob(
			JobRepository jobRepository,
			Step userGenerateStep,
			UserGenerateJobLoggingListener userGenerateJobLoggingListener) {
		return new JobBuilder("userGenerateJob", jobRepository)
				.start(userGenerateStep)
				.listener(userGenerateJobLoggingListener)
				.build();
	}

}
