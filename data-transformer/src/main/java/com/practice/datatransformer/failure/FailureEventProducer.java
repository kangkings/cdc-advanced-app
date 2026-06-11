package com.practice.datatransformer.failure;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.practice.datatransformer.observability.TransformerMetrics;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@Component
public class FailureEventProducer {

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final ObjectMapper objectMapper;
	private final MeterRegistry meterRegistry;
	private final String dlqTopic;

	public FailureEventProducer(
			KafkaTemplate<String, String> kafkaTemplate,
			ObjectMapper objectMapper,
			MeterRegistry meterRegistry,
			@Value("${cdc.kafka.dlq-topic}") String dlqTopic) {
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
		this.meterRegistry = meterRegistry;
		this.dlqTopic = dlqTopic;
	}

	// 실패 이벤트를 DLQ에 동기 발행해 실패 기록 유실 방지
	public void publish(FailureEvent event) throws Exception {
		String key = createKey(event);
		String payload = objectMapper.writeValueAsString(event);
		kafkaTemplate.send(dlqTopic, key, payload)
				.whenComplete((result, exception) -> handleResult(event, key, result, exception))
				.join();
	}

	private String createKey(FailureEvent event) {
		return "%s:%s:%s".formatted(event.stage(), event.failureType(), event.sourceOffset());
	}

	private void handleResult(FailureEvent event, String key, SendResult<String, String> result, Throwable exception) {
		String status = exception == null ? TransformerMetrics.Status.SUCCESS : TransformerMetrics.Status.FAILED;
		meterRegistry.counter(
				TransformerMetrics.Names.DLQ_PUBLISH_COUNT,
				TransformerMetrics.Tags.MODULE, TransformerMetrics.MODULE,
				TransformerMetrics.Tags.TOPIC, dlqTopic,
				TransformerMetrics.Tags.FAILURE_TYPE, event.failureType().name(),
				TransformerMetrics.Tags.STATUS, status)
				.increment();

		if (exception == null) {
			log.info("[TRANSFORM-DLQ][PUBLISH][SUCCESS] topic={}, key={}, failureType={}",
					dlqTopic,
					key,
					event.failureType());
			return;
		}

		log.error("[TRANSFORM-DLQ][PUBLISH][FAILED] topic={}, key={}, failureType={}",
				dlqTopic,
				key,
				event.failureType(),
				exception);
	}

}
