package com.practice.dataloader.pipeline;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.practice.dataloader.model.TransformEvent;
import com.practice.dataloader.observability.LoaderMetrics;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransformEventListener {

	private final ObjectMapper objectMapper;
	private final LoadService loadService;
	private final MeterRegistry meterRegistry;

	@KafkaListener(
			topics = "${cdc.kafka.input-topic}",
			groupId = "${spring.kafka.consumer.group-id}",
			containerFactory = "kafkaListenerContainerFactory")
	public void listen(List<String> messages) {
		LocalDateTime startTime = LocalDateTime.now();
		String status = LoaderMetrics.Status.SUCCESS;
		try {
			List<TransformEvent> events = messages.stream()
					.map(this::deserialize)
					.toList();
			loadService.loadBatch(events);
		}
		catch (Exception ex) {
			status = LoaderMetrics.Status.FAILED;
			log.error("[LOAD][BATCH][FAILED] batchSize={}", messages.size(), ex);
			throw ex;
		}
		finally {
			meterRegistry.counter(
					LoaderMetrics.Names.KAFKA_CONSUME_COUNT,
					LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE,
					LoaderMetrics.Tags.STATUS, status).increment(messages.size());
			io.micrometer.core.instrument.Timer.builder(LoaderMetrics.Names.KAFKA_LISTENER_DURATION)
					.description("Data loader Kafka batch listener duration")
					.tag(LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE)
					.tag(LoaderMetrics.Tags.STATUS, status)
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
