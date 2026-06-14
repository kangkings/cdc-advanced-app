package com.practice.logscanner.batch.step;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import com.practice.logscanner.batch.listener.RedoLogPrintStepLoggingListener;
import com.practice.logscanner.batch.listener.RedoLogPrintWriteLoggingListener;
import com.practice.logscanner.batch.model.RedoLogEntry;
import com.practice.logscanner.batch.reader.RedoLogItemReader;
import com.practice.logscanner.batch.writer.RedoLogItemWriter;
import com.practice.logscanner.logminer.LogMinerCheckpointRepository;
import com.practice.logscanner.logminer.LogMinerConnectionFactory;
import com.practice.logscanner.logminer.LogMinerTargetProperties;
import com.practice.logscanner.logminer.PendingTransactionRepository;
import com.practice.logscanner.logminer.RedoLogFileRegistrar;

import io.micrometer.core.instrument.MeterRegistry;

@Configuration
public class RedoLogPrintStepConfig {

	@Bean
	public Step redoLogPrintStep(
			JobRepository jobRepository,
			PlatformTransactionManager transactionManager,
			RedoLogItemReader redoLogItemReader,
			RedoLogItemWriter redoLogItemWriter,
			RedoLogPrintStepLoggingListener redoLogPrintStepLoggingListener,
			RedoLogPrintWriteLoggingListener redoLogPrintWriteLoggingListener,
			@Value("${batch.redo-log-print.chunk-size:100}") int chunkSize) {
		return new StepBuilder("redoLogPrintStep", jobRepository)
				.<RedoLogEntry, RedoLogEntry>chunk(chunkSize, transactionManager)
				.reader(redoLogItemReader)
				.writer(redoLogItemWriter)
				.stream(redoLogItemReader)
				.listener(redoLogPrintStepLoggingListener)
				.listener(redoLogPrintWriteLoggingListener)
				.build();
	}

	@Bean
	@StepScope
	public RedoLogItemReader redoLogItemReader(
			LogMinerConnectionFactory logMinerConnectionFactory,
			LogMinerCheckpointRepository checkpointRepository,
			RedoLogFileRegistrar redoLogFileRegistrar,
			PendingTransactionRepository pendingTransactionRepository,
			LogMinerTargetProperties logMinerTargetProperties,
			MeterRegistry meterRegistry) {
		return new RedoLogItemReader(
				logMinerConnectionFactory,
				checkpointRepository,
				redoLogFileRegistrar,
				pendingTransactionRepository,
				logMinerTargetProperties,
				meterRegistry);
	}

}
