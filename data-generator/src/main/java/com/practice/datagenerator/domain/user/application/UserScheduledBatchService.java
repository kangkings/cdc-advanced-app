package com.practice.datagenerator.domain.user.application;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class UserScheduledBatchService implements DisposableBean {

	private final JobLauncher jobLauncher;
	private final Job userGenerateJob;

	private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
	private final AtomicBoolean running = new AtomicBoolean(false);
	private Future<?> scheduledFuture;
	private int currentCount;

	public UserScheduledBatchService(
			JobLauncher jobLauncher,
			@Qualifier("userGenerateJob") Job userGenerateJob) {
		this.jobLauncher = jobLauncher;
		this.userGenerateJob = userGenerateJob;
	}

	public void start(int count) {
		if (running.compareAndSet(false, true)) {
			currentCount = count;
			scheduledFuture = scheduler.scheduleAtFixedRate(this::runBatch, 0, 1, TimeUnit.SECONDS);
			log.info("[SCHEDULED-BATCH] 시작 count={}/초", count);
		} else {
			log.warn("[SCHEDULED-BATCH] 이미 실행 중입니다. count={}", currentCount);
		}
	}

	public void stop() {
		if (running.compareAndSet(true, false)) {
			if (scheduledFuture != null) {
				scheduledFuture.cancel(false);
			}
			log.info("[SCHEDULED-BATCH] 중단 요청. 현재 실행 중인 배치는 완료 후 종료됩니다.");
		} else {
			log.warn("[SCHEDULED-BATCH] 실행 중이지 않습니다.");
		}
	}

	public boolean isRunning() {
		return running.get();
	}

	private void runBatch() {
		try {
			JobParameters jobParameters = new JobParametersBuilder()
					.addLong("count", (long) currentCount)
					.addString("requestId", UUID.randomUUID().toString())
					.addLong("requestedAt", System.currentTimeMillis())
					.toJobParameters();

			jobLauncher.run(userGenerateJob, jobParameters);
		} catch (Exception ex) {
			log.error("[SCHEDULED-BATCH] 배치 실행 실패", ex);
		}
	}

	@Override
	public void destroy() {
		stop();
		scheduler.shutdown();
	}

}
