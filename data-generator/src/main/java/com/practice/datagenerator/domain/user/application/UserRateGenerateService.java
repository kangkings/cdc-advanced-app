package com.practice.datagenerator.domain.user.application;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Service;

import com.practice.datagenerator.domain.user.infrastructure.UserJdbcBulkInserter;
import com.practice.datagenerator.observability.GeneratorMetrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class UserRateGenerateService implements DisposableBean {

	private static final String STATUS_IDLE = "IDLE";
	private static final String STATUS_RUNNING = "RUNNING";
	private static final String STATUS_COMPLETED = "COMPLETED";
	private static final String STATUS_STOPPED = "STOPPED";
	private static final String STATUS_FAILED = "FAILED";

	private final UserJdbcBulkInserter userJdbcBulkInserter;
	private final MeterRegistry meterRegistry;
	private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
	private final AtomicBoolean running = new AtomicBoolean(false);
	private final AtomicLong generatedCount = new AtomicLong();
	private final AtomicLong failedCount = new AtomicLong();

	private Future<?> scheduledFuture;
	private volatile String status = STATUS_IDLE;
	private volatile int rate;
	private volatile int durationSeconds;
	private volatile long expectedTotal;
	private volatile LocalDateTime startedAt;
	private volatile LocalDateTime endedAt;
	private volatile String lastError;

	public UserRateGenerateService(
			UserJdbcBulkInserter userJdbcBulkInserter,
			MeterRegistry meterRegistry) {
		this.userJdbcBulkInserter = userJdbcBulkInserter;
		this.meterRegistry = meterRegistry;
	}

	public synchronized UserRateGenerateStatus start(int rate, int durationSeconds) {
		validate(rate, durationSeconds);

		if (!running.compareAndSet(false, true)) {
			throw new IllegalStateException("Rate generator is already running.");
		}

		this.rate = rate;
		this.durationSeconds = durationSeconds;
		this.expectedTotal = (long) rate * durationSeconds;
		this.generatedCount.set(0);
		this.failedCount.set(0);
		this.startedAt = LocalDateTime.now();
		this.endedAt = null;
		this.lastError = null;
		this.status = STATUS_RUNNING;

		log.info("[RATE-GENERATE][START] rate={}, durationSeconds={}, expectedTotal={}",
				rate, durationSeconds, expectedTotal);

		scheduledFuture = scheduler.scheduleAtFixedRate(this::runTick, 0, 1, TimeUnit.SECONDS);
		return status();
	}

	public synchronized UserRateGenerateStatus stop() {
		if (running.compareAndSet(true, false)) {
			cancelFuture();
			endedAt = LocalDateTime.now();
			status = STATUS_STOPPED;
			log.info("[RATE-GENERATE][STOP] generatedCount={}, failedCount={}",
					generatedCount.get(), failedCount.get());
		}
		return status();
	}

	public UserRateGenerateStatus status() {
		LocalDateTime now = LocalDateTime.now();
		LocalDateTime baseTime = endedAt != null ? endedAt : now;
		long elapsedSeconds = startedAt == null ? 0 : Duration.between(startedAt, baseTime).toSeconds();

		return new UserRateGenerateStatus(
				running.get(),
				status,
				rate,
				durationSeconds,
				expectedTotal,
				generatedCount.get(),
				failedCount.get(),
				startedAt,
				endedAt,
				elapsedSeconds,
				lastError);
	}

	private void runTick() {
		if (!running.get()) {
			return;
		}

		if (Duration.between(startedAt, LocalDateTime.now()).toSeconds() >= durationSeconds) {
			complete();
			return;
		}

		LocalDateTime tickStart = LocalDateTime.now();
		try {
			userJdbcBulkInserter.insertUsers(rate);
			long total = generatedCount.addAndGet(rate);
			meterRegistry.counter(
					GeneratorMetrics.Names.RATE_TICK_COUNT,
					GeneratorMetrics.Tags.MODULE, GeneratorMetrics.MODULE,
					GeneratorMetrics.Tags.STATUS, GeneratorMetrics.Status.SUCCESS).increment();
			meterRegistry.counter(
					GeneratorMetrics.Names.RATE_INSERT_COUNT,
					GeneratorMetrics.Tags.MODULE, GeneratorMetrics.MODULE).increment(rate);
			Timer.builder(GeneratorMetrics.Names.RATE_INSERT_DURATION)
					.description("Rate generator insert duration")
					.tag(GeneratorMetrics.Tags.MODULE, GeneratorMetrics.MODULE)
					.tag(GeneratorMetrics.Tags.STATUS, GeneratorMetrics.Status.SUCCESS)
					.register(meterRegistry)
					.record(Duration.between(tickStart, LocalDateTime.now()));
			log.info("[RATE-GENERATE][TICK] inserted={}, totalInserted={}, elapsedMs={}",
					rate, total, Duration.between(tickStart, LocalDateTime.now()).toMillis());
		} catch (Exception ex) {
			failedCount.incrementAndGet();
			meterRegistry.counter(
					GeneratorMetrics.Names.RATE_TICK_COUNT,
					GeneratorMetrics.Tags.MODULE, GeneratorMetrics.MODULE,
					GeneratorMetrics.Tags.STATUS, GeneratorMetrics.Status.FAILED).increment();
			meterRegistry.counter(
					GeneratorMetrics.Names.RATE_FAILURE_COUNT,
					GeneratorMetrics.Tags.MODULE, GeneratorMetrics.MODULE).increment();
			fail(ex);
		}
	}

	private synchronized void complete() {
		if (running.compareAndSet(true, false)) {
			cancelFuture();
			endedAt = LocalDateTime.now();
			status = STATUS_COMPLETED;
			log.info("[RATE-GENERATE][END] status={}, generatedCount={}, expectedTotal={}, failedCount={}",
					status, generatedCount.get(), expectedTotal, failedCount.get());
		}
	}

	private synchronized void fail(Exception ex) {
		if (running.compareAndSet(true, false)) {
			cancelFuture();
			endedAt = LocalDateTime.now();
			status = STATUS_FAILED;
			lastError = ex.getMessage();
			log.error("[RATE-GENERATE][FAILED] rate={}, generatedCount={}, failedCount={}, message={}",
					rate, generatedCount.get(), failedCount.get(), ex.getMessage(), ex);
		}
	}

	private void cancelFuture() {
		if (scheduledFuture != null) {
			scheduledFuture.cancel(false);
			scheduledFuture = null;
		}
	}

	private void validate(int rate, int durationSeconds) {
		if (rate <= 0) {
			throw new IllegalArgumentException("rate는 1 이상이어야 합니다.");
		}
		if (durationSeconds <= 0) {
			throw new IllegalArgumentException("durationSeconds는 1 이상이어야 합니다.");
		}
	}

	@Override
	public void destroy() {
		stop();
		scheduler.shutdown();
	}
}
