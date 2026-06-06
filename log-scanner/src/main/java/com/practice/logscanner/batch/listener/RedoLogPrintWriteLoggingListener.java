package com.practice.logscanner.batch.listener;

import org.springframework.batch.core.listener.ItemWriteListener;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.stereotype.Component;

import com.practice.logscanner.batch.model.RedoLogEntry;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class RedoLogPrintWriteLoggingListener implements ItemWriteListener<RedoLogEntry> {

	private static final String MODULE = "log-scanner";
	private static final String WRITE_DURATION = "log_scanner.redo_log.writer.duration";
	private static final String WRITE_CHUNK_COUNT = "log_scanner.redo_log.writer.chunk.count";
	private static final String WRITE_ITEM_COUNT = "log_scanner.redo_log.writer.item.count";
	private static final String WRITE_ERROR_COUNT = "log_scanner.redo_log.writer.error.count";

	private final MeterRegistry meterRegistry;
	private final ThreadLocal<Timer.Sample> sampleHolder = new ThreadLocal<>();

	@Override
	public void beforeWrite(Chunk<? extends RedoLogEntry> items) {
		sampleHolder.set(Timer.start(meterRegistry));
	}

	@Override
	public void afterWrite(Chunk<? extends RedoLogEntry> items) {
		stopTimer("SUCCESS");
		meterRegistry.counter(WRITE_CHUNK_COUNT, "module", MODULE, "status", "SUCCESS").increment();
		meterRegistry.counter(WRITE_ITEM_COUNT, "module", MODULE, "status", "SUCCESS").increment(items.size());
	}

	@Override
	public void onWriteError(Exception exception, Chunk<? extends RedoLogEntry> items) {
		stopTimer("FAILED");
		meterRegistry.counter(WRITE_ERROR_COUNT, "module", MODULE).increment();
		log.error("[REDO-LOG-PRINT][WRITER][ERROR] chunkSize={}", items.size(), exception);
	}

	private void stopTimer(String status) {
		Timer.Sample sample = sampleHolder.get();
		if (sample == null) {
			return;
		}
		sample.stop(Timer.builder(WRITE_DURATION)
				.description("Redo log print item writer duration")
				.tag("module", MODULE)
				.tag("status", status)
				.register(meterRegistry));
		sampleHolder.remove();
	}

}
