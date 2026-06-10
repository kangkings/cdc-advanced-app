package com.practice.datatransformer.pipeline;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.practice.datatransformer.model.TransformEvent;
import com.practice.datatransformer.observability.TransformerMetrics;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@Component
public class TransformProducer {

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final ObjectMapper objectMapper;
	private final MeterRegistry meterRegistry;
	private final String outputTopic;

	public TransformProducer(
			KafkaTemplate<String, String> kafkaTemplate,
			ObjectMapper objectMapper,
			MeterRegistry meterRegistry,
			@Value("${cdc.kafka.output-topic}") String outputTopic) {
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
		this.meterRegistry = meterRegistry;
		this.outputTopic = outputTopic;
	}

	public void publish(TransformEvent event) throws Exception {
		String key = createKey(event);
		String payload = objectMapper.writeValueAsString(event);
		kafkaTemplate.send(outputTopic, key, payload)
				.whenComplete((result, exception) -> handleResult(key, result, exception));
	}

	private String createKey(TransformEvent event) {
		return "%d:%d".formatted(event.entry().scn(), event.entry().rowNumber());
	}

	private void handleResult(String key, SendResult<String, String> result, Throwable exception) {
		String status = exception == null ? TransformerMetrics.Status.SUCCESS : TransformerMetrics.Status.FAILED;
		meterRegistry.counter(
				TransformerMetrics.Names.KAFKA_PUBLISH_COUNT,
				TransformerMetrics.Tags.MODULE, TransformerMetrics.MODULE,
				TransformerMetrics.Tags.TOPIC, outputTopic,
				TransformerMetrics.Tags.STATUS, status)
				.increment();

		if (exception == null) {
			log.debug("[TRANSFORM-PUBLISH][SUCCESS] topic={}, key={}", outputTopic, key);
			return;
		}

		log.error("[TRANSFORM-PUBLISH][FAILED] topic={}, key={}", outputTopic, key, exception);
	}

}
