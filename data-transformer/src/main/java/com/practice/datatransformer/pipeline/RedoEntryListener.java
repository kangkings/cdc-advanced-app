package com.practice.datatransformer.pipeline;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.practice.datatransformer.model.RedoEntry;
import com.practice.datatransformer.model.TransformEvent;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@Component
@RequiredArgsConstructor
public class RedoEntryListener {

	private static final String MODULE = "data-transformer";
	private static final String CONSUME_COUNT = "data_transformer.kafka.consume.count";
	private static final String PROCESS_DURATION = "data_transformer.pipeline.process.duration";

	private final ObjectMapper objectMapper;
	private final TransformService transformService;
	private final TransformProducer transformProducer;
	private final MeterRegistry meterRegistry;

	@KafkaListener(
			topics = "${cdc.kafka.input-topic}",
			groupId = "${spring.kafka.consumer.group-id}",
			containerFactory = "kafkaListenerContainerFactory")
	public void listen(String message) throws Exception {
		Timer.Sample sample = Timer.start(meterRegistry);
		String status = "SUCCESS";

		try {
			RedoEntry entry = objectMapper.readValue(message, RedoEntry.class);
			TransformEvent event = transformService.transform(entry);
			transformProducer.publish(event);
			log.info("[TRANSFORM][DONE] scn={}, row={}, operation={}, table={}, valid={}, rowExists={}",
					entry.scn(),
					entry.rowNumber(),
					entry.operation(),
					entry.tableName(),
					event.check().valid(),
					event.check().rowExists());
		}
		catch (Exception ex) {
			status = "FAILED";
			log.error("[TRANSFORM][FAILED] message={}", message, ex);
			throw ex;
		}
		finally {
			meterRegistry.counter(CONSUME_COUNT, "module", MODULE, "status", status).increment();
			sample.stop(Timer.builder(PROCESS_DURATION)
					.description("Data transformer pipeline process duration")
					.tag("module", MODULE)
					.tag("status", status)
					.register(meterRegistry));
		}
	}

}
