package com.practice.datagenerator.batch.step;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import com.practice.datagenerator.batch.listener.UserGenerateProcessLoggingListener;
import com.practice.datagenerator.batch.listener.UserGenerateStepLoggingListener;
import com.practice.datagenerator.batch.reader.UserGenerateItemReader;
import com.practice.datagenerator.domain.user.domain.entity.User;

import io.micrometer.core.instrument.MeterRegistry;

@Configuration
public class UserGenerateStepConfig {

	private static final int USER_GENERATE_CHUNK_SIZE = 100;

	@Bean
	public Step userGenerateStep(
			JobRepository jobRepository,
			PlatformTransactionManager transactionManager,
			ItemReader<Integer> userGenerateItemReader,
			ItemProcessor<Integer, User> userGenerateItemProcessor,
			ItemWriter<User> userGenerateItemWriter,
			UserGenerateStepLoggingListener userGenerateStepLoggingListener,
			UserGenerateProcessLoggingListener userGenerateProcessLoggingListener) {
		return new StepBuilder("userGenerateStep", jobRepository)
				.<Integer, User>chunk(USER_GENERATE_CHUNK_SIZE, transactionManager)
				.reader(userGenerateItemReader)
				.processor(userGenerateItemProcessor)
				.writer(userGenerateItemWriter)
				.listener(userGenerateStepLoggingListener)
				.listener(userGenerateProcessLoggingListener)
				.build();
	}

	@Bean
	@StepScope
	public UserGenerateItemReader userGenerateItemReader(
			@Value("#{jobParameters['count'] ?: 0}") Long count,
			MeterRegistry meterRegistry) {
		return new UserGenerateItemReader(count.intValue(), meterRegistry);
	}

}
