package com.practice.logscanner.batch.listener;

import org.springframework.batch.core.listener.ItemWriteListener;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.stereotype.Component;

import com.practice.logscanner.batch.model.RedoLogEntry;
import com.practice.logscanner.observability.LogScannerMetrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class RedoLogPrintWriteLoggingListener implements ItemWriteListener<RedoLogEntry> {

	private final MeterRegistry meterRegistry;
	private final ThreadLocal<Timer.Sample> sampleHolder = new ThreadLocal<>();

	@Override
	public void beforeWrite(Chunk<? extends RedoLogEntry> items) {
		sampleHolder.set(Timer.start(meterRegistry));
	}

	@Override
	public void afterWrite(Chunk<? extends RedoLogEntry> items) {
		stopTimer(LogScannerMetrics.Status.SUCCESS);
		meterRegistry.counter(
				LogScannerMetrics.Names.REDO_LOG_WRITER_CHUNK_COUNT,
				LogScannerMetrics.Tags.MODULE, LogScannerMetrics.MODULE,
				LogScannerMetrics.Tags.STATUS, LogScannerMetrics.Status.SUCCESS).increment();
		meterRegistry.counter(
				LogScannerMetrics.Names.REDO_LOG_WRITER_ITEM_COUNT,
				LogScannerMetrics.Tags.MODULE, LogScannerMetrics.MODULE,
				LogScannerMetrics.Tags.STATUS, LogScannerMetrics.Status.SUCCESS).increment(items.size());
	}

	@Override
	public void onWriteError(Exception exception, Chunk<? extends RedoLogEntry> items) {
		stopTimer(LogScannerMetrics.Status.FAILED);
		meterRegistry.counter(
				LogScannerMetrics.Names.REDO_LOG_WRITER_ERROR_COUNT,
				LogScannerMetrics.Tags.MODULE, LogScannerMetrics.MODULE).increment();
		log.error("[REDO-LOG-PRINT][WRITER][ERROR] chunkSize={}", items.size(), exception);
	}

	private void stopTimer(String status) {
		Timer.Sample sample = sampleHolder.get();
		if (sample == null) {
			return;
		}
		sample.stop(Timer.builder(LogScannerMetrics.Names.REDO_LOG_WRITER_DURATION)
				.description("Redo log print item writer duration")
				.tag(LogScannerMetrics.Tags.MODULE, LogScannerMetrics.MODULE)
				.tag(LogScannerMetrics.Tags.STATUS, status)
				.register(meterRegistry));
		sampleHolder.remove();
	}

}
