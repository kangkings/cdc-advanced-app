package com.practice.dataloader.pipeline;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.practice.dataloader.model.TransformEvent;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransformEventListener {

	private static final String MODULE = "data-loader";
	private static final String CONSUME_COUNT = "data_loader.kafka.consume.count";
	private static final String LISTENER_DURATION = "data_loader.kafka.listener.duration";

	private final ObjectMapper objectMapper;
	private final LoadService loadService;
	private final MeterRegistry meterRegistry;

	@KafkaListener(
			topics = "${cdc.kafka.input-topic}",
			groupId = "${spring.kafka.consumer.group-id}",
			containerFactory = "kafkaListenerContainerFactory")
	public void listen(List<String> messages) {
		LocalDateTime startTime = LocalDateTime.now();
		String status = "SUCCESS";
		try {
			List<TransformEvent> events = messages.stream()
					.map(this::deserialize)
					.toList();
			loadService.loadBatch(events);
		}
		catch (Exception ex) {
			status = "FAILED";
			log.error("[LOAD][BATCH][FAILED] batchSize={}", messages.size(), ex);
			throw ex;
		}
		finally {
			meterRegistry.counter(CONSUME_COUNT, "module", MODULE, "status", status).increment(messages.size());
			io.micrometer.core.instrument.Timer.builder(LISTENER_DURATION)
					.description("Data loader Kafka batch listener duration")
					.tag("module", MODULE)
					.tag("status", status)
					.register(meterRegistry)
					.record(Duration.between(startTime, LocalDateTime.now()));
		}
	}

	private TransformEvent deserialize(String message) {
		try {
			return objectMapper.readValue(message, TransformEvent.class);
		}
		catch (Exception ex) {
			throw new IllegalArgumentException("Failed to deserialize message: " + message, ex);
		}
	}

}
