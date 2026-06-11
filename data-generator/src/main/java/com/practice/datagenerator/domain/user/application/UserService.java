package com.practice.datagenerator.domain.user.application;

import java.util.UUID;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
// 요청 건수만큼 User 생성 Batch 실행
public class UserService implements UserUsecase {

	private final JobLauncher jobLauncher;

	@Qualifier("userGenerateJob")
	private final Job userGenerateJob;

	@Override
	public UserGenerateResult generateUsers(int count) {
		validateGenerateParameters(count);

		try {
			JobParameters jobParameters = new JobParametersBuilder()
				.addLong("count", (long) count)
				.addString("requestId", UUID.randomUUID().toString())
				.addLong("requestedAt", System.currentTimeMillis())
				.toJobParameters();

			JobExecution jobExecution = jobLauncher.run(userGenerateJob, jobParameters);
			return new UserGenerateResult(jobExecution.getId(), jobExecution.getStatus().name(), count);

		}
		catch (Exception ex) {
			log.error("[USER-GENERATE][REQUEST][FAILED] count={}, errorType={}, message={}",
					count,
					ex.getClass().getName(),
					ex.getMessage(),
					ex);
			throw new IllegalStateException("회원 생성 실패", ex);
		}
	}

	private void validateGenerateParameters(int count) {
		if (count <= 0) {
			throw new IllegalArgumentException("삽입 건수는 1 이상이어야 합니다.");
		}
	}

}
