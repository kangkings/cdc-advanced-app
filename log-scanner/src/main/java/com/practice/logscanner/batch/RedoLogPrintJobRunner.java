package com.practice.logscanner.batch;

import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class RedoLogPrintJobRunner implements ApplicationRunner, DisposableBean {

	private final boolean enabled;
	private final JobLauncher jobLauncher;
	private final Job redoLogPrintJob;

	private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

	public RedoLogPrintJobRunner(
			@Value("${batch.redo-log-print.enabled:true}") boolean enabled,
			JobLauncher jobLauncher,
			Job redoLogPrintJob) {
		this.enabled = enabled;
		this.jobLauncher = jobLauncher;
		this.redoLogPrintJob = redoLogPrintJob;
	}

	@Override
	public void run(ApplicationArguments args) {
		if (!enabled) {
			log.info("[REDO-LOG-PRINT] 배치가 비활성화되어 있습니다.");
			return;
		}

		log.info("[REDO-LOG-PRINT] 3초 간격 로그 수집 시작.");
		scheduler.scheduleAtFixedRate(this::runBatch, 0, 3, TimeUnit.SECONDS);
	}

	private void runBatch() {
		try {
			JobParameters parameters = new JobParametersBuilder()
					.addString("requestedAt", Instant.now().toString())
					.toJobParameters();

			jobLauncher.run(redoLogPrintJob, parameters);
		}
		catch (Exception ex) {
			log.error("[REDO-LOG-PRINT] 로그 수집 실패.", ex);
		}
	}

	@Override
	public void destroy() {
		log.info("[REDO-LOG-PRINT] 로그 스캐너 종료 중...");
		scheduler.shutdown();
	}

}
