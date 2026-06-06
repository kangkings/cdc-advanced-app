package com.practice.dataloader.pipeline;

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

	private final ObjectMapper objectMapper;
	private final LoadService loadService;
	private final MeterRegistry meterRegistry;

	@KafkaListener(
			topics = "${cdc.kafka.input-topic}",
			groupId = "${spring.kafka.consumer.group-id}",
			containerFactory = "kafkaListenerContainerFactory")
	public void listen(String message) throws Exception {
		String status = "SUCCESS";
		try {
			TransformEvent event = objectMapper.readValue(message, TransformEvent.class);
			loadService.load(event);
		}
		catch (Exception ex) {
			status = "FAILED";
			log.error("[LOAD][FAILED] message={}", message, ex);
			throw ex;
		}
		finally {
			meterRegistry.counter(CONSUME_COUNT, "module", MODULE, "status", status).increment();
		}
	}

}
