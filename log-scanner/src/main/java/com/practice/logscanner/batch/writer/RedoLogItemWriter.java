package com.practice.logscanner.batch.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.practice.logscanner.batch.model.RedoLogEntry;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@Component
public class RedoLogItemWriter implements ItemWriter<RedoLogEntry> {

	private static final String MODULE = "log-scanner";
	private static final String PUBLISH_DURATION = "log_scanner.kafka.publish.chunk.duration";
	private static final String PUBLISH_CHUNK_COUNT = "log_scanner.kafka.publish.chunk.count";
	private static final String PUBLISH_ENTRY_COUNT = "log_scanner.kafka.publish.entry.count";
	private static final String PUBLISH_SUCCESS_COUNT = "log_scanner.kafka.publish.success.count";
	private static final String PUBLISH_FAILURE_COUNT = "log_scanner.kafka.publish.failure.count";

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final ObjectMapper objectMapper;
	private final MeterRegistry meterRegistry;
	private final String topic;

	public RedoLogItemWriter(
			KafkaTemplate<String, String> kafkaTemplate,
			ObjectMapper objectMapper,
			MeterRegistry meterRegistry,
			@Value("${cdc.kafka.topic}") String topic) {
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
		this.meterRegistry = meterRegistry;
		this.topic = topic;
	}

	@Override
	public void write(Chunk<? extends RedoLogEntry> chunk) throws Exception {
		Timer.Sample sample = Timer.start(meterRegistry);
		AtomicInteger failures = new AtomicInteger();
		List<CompletableFuture<Void>> futures = new ArrayList<>(chunk.size());

		log.info("[REDO-LOG-PUBLISH][WRITER][START] topic={}, chunkSize={}", topic, chunk.size());

		for (RedoLogEntry entry : chunk) {
			String key = createKey(entry);
			String payload = objectMapper.writeValueAsString(entry);
			CompletableFuture<Void> future = kafkaTemplate.send(topic, key, payload)
					.whenComplete((result, exception) -> handleSendResult(entry, result, exception, failures))
					.handle((result, exception) -> null);
			futures.add(future);
			meterRegistry.counter(PUBLISH_ENTRY_COUNT, "module", MODULE, "topic", topic).increment();
		}

		CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();

		String status = failures.get() == 0 ? "SUCCESS" : "FAILED";
		sample.stop(Timer.builder(PUBLISH_DURATION)
				.description("Kafka redo log publish chunk duration")
				.tag("module", MODULE)
				.tag("topic", topic)
				.tag("status", status)
				.register(meterRegistry));
		meterRegistry.counter(PUBLISH_CHUNK_COUNT, "module", MODULE, "topic", topic, "status", status).increment();

		log.info("[REDO-LOG-PUBLISH][WRITER][END] topic={}, chunkSize={}, failures={}",
				topic,
				chunk.size(),
				failures.get());

		if (failures.get() > 0) {
			throw new IllegalStateException("Failed to publish redo log entries. topic=%s, failures=%d"
					.formatted(topic, failures.get()));
		}
	}

	private String createKey(RedoLogEntry entry) {
		return "%d:%d".formatted(entry.scn(), entry.rowNumber());
	}

	private void handleSendResult(
			RedoLogEntry entry,
			SendResult<String, String> result,
			Throwable exception,
			AtomicInteger failures) {
		if (exception == null) {
			meterRegistry.counter(PUBLISH_SUCCESS_COUNT, "module", MODULE, "topic", topic).increment();
			return;
		}

		failures.incrementAndGet();
		meterRegistry.counter(PUBLISH_FAILURE_COUNT, "module", MODULE, "topic", topic).increment();
		log.error("[REDO-LOG-PUBLISH][WRITER][ERROR] topic={}, key={}, scn={}, row={}, operation={}, owner={}, table={}",
				topic,
				createKey(entry),
				entry.scn(),
				entry.rowNumber(),
				entry.operation(),
				entry.owner(),
				entry.tableName(),
				exception);
	}

}
