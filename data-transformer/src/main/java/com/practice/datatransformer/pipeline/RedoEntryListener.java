package com.practice.datatransformer.pipeline;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.practice.datatransformer.failure.FailureEvent;
import com.practice.datatransformer.failure.FailureEventProducer;
import com.practice.datatransformer.failure.FailureType;
import com.practice.datatransformer.model.RedoEntry;
import com.practice.datatransformer.model.TransformEvent;
import com.practice.datatransformer.observability.TransformerMetrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@Component
@RequiredArgsConstructor
public class RedoEntryListener {

	private final ObjectMapper objectMapper;
	private final TransformService transformService;
	private final TransformProducer transformProducer;
	private final FailureEventProducer failureEventProducer;
	private final MeterRegistry meterRegistry;

	@KafkaListener(
			topics = "${cdc.kafka.input-topic}",
			groupId = "${spring.kafka.consumer.group-id}",
			containerFactory = "kafkaListenerContainerFactory")
	public void listen(ConsumerRecord<String, String> record) throws Exception {
		Timer.Sample sample = Timer.start(meterRegistry);
		String status = TransformerMetrics.Status.SUCCESS;
		String message = record.value();

		try {
			RedoEntry entry = deserialize(record);
			TransformEvent event = transformService.transform(entry);
			if (!event.check().valid() || !event.check().supported()) {
				publishFailure(record, event);
				status = TransformerMetrics.Status.DLQ;
				return;
			}

			transformProducer.publish(event);
			log.info("[TRANSFORM][DONE] scn={}, row={}, operation={}, table={}, valid={}, rowExists={}",
					entry.scn(),
					entry.rowNumber(),
					entry.operation(),
					entry.tableName(),
					event.check().valid(),
					event.check().rowExists());
		}
		catch (NonRetryableTransformHandledException ex) {
			status = TransformerMetrics.Status.DLQ;
			log.warn("[TRANSFORM][DLQ] topic={}, partition={}, offset={}, reason={}",
					record.topic(),
					record.partition(),
					record.offset(),
					ex.getCause() == null ? ex.getMessage() : ex.getCause().getMessage());
		}
		catch (Exception ex) {
			status = TransformerMetrics.Status.FAILED;
			log.error("[TRANSFORM][FAILED] topic={}, partition={}, offset={}, message={}",
					record.topic(),
					record.partition(),
					record.offset(),
					message,
					ex);
			throw ex;
		}
		finally {
			meterRegistry.counter(
					TransformerMetrics.Names.KAFKA_CONSUME_COUNT,
					TransformerMetrics.Tags.MODULE, TransformerMetrics.MODULE,
					TransformerMetrics.Tags.STATUS, status).increment();
			sample.stop(Timer.builder(TransformerMetrics.Names.PIPELINE_PROCESS_DURATION)
					.description("Data transformer pipeline process duration")
					.tag(TransformerMetrics.Tags.MODULE, TransformerMetrics.MODULE)
					.tag(TransformerMetrics.Tags.STATUS, status)
					.register(meterRegistry));
		}
	}

	// 원본 Kafka 메시지를 RedoEntry로 변환하고 역직렬화 실패는 DLQ로 분리
	private RedoEntry deserialize(ConsumerRecord<String, String> record) throws Exception {
		try {
			return objectMapper.readValue(record.value(), RedoEntry.class);
		}
		catch (Exception ex) {
			FailureEvent failureEvent = FailureEvent.of(
					FailureType.DESERIALIZATION_FAILED,
					false,
					ex.getMessage(),
					record.topic(),
					record.partition(),
					record.offset(),
					record.value(),
					Map.of());
			publishFailure(failureEvent);
			throw new NonRetryableTransformHandledException(ex);
		}
	}

	// 변환 실패 결과를 공용 FailureEvent로 만들어 DLQ 발행
	private void publishFailure(ConsumerRecord<String, String> record, TransformEvent event) throws Exception {
		FailureType failureType = classify(event);
		FailureEvent failureEvent = FailureEvent.of(
				failureType,
				false,
				event.check().reason(),
				record.topic(),
				record.partition(),
				record.offset(),
				record.value(),
				context(event));
		publishFailure(failureEvent);
		log.warn("[TRANSFORM][DLQ] failureType={}, reason={}, topic={}, partition={}, offset={}",
				failureType,
				event.check().reason(),
				record.topic(),
				record.partition(),
				record.offset());
	}

	private void publishFailure(FailureEvent failureEvent) throws Exception {
		meterRegistry.counter(
				TransformerMetrics.Names.FAILURE_COUNT,
				TransformerMetrics.Tags.MODULE, TransformerMetrics.MODULE,
				TransformerMetrics.Tags.FAILURE_TYPE, failureEvent.failureType().name(),
				TransformerMetrics.Tags.RETRYABLE, Boolean.toString(failureEvent.retryable()))
				.increment();
		failureEventProducer.publish(failureEvent);
	}

	private FailureType classify(TransformEvent event) {
		String reason = event.check().reason();
		if (reason == null) {
			return FailureType.UNKNOWN_TRANSFORM_FAILED;
		}
		if (reason.startsWith("unsupported table")) {
			return FailureType.UNSUPPORTED_TABLE;
		}
		if (reason.startsWith("unsupported operation")) {
			return FailureType.UNSUPPORTED_OPERATION;
		}
		if (reason.contains("required")) {
			return FailureType.MISSING_REQUIRED_FIELD;
		}
		if (reason.contains("ROWID")) {
			return FailureType.SOURCE_KEY_LOOKUP_FAILED;
		}
		if (reason.contains("payload data was not parsed")) {
			return FailureType.PAYLOAD_PARSE_FAILED;
		}
		return FailureType.UNKNOWN_TRANSFORM_FAILED;
	}

	private Map<String, String> context(TransformEvent event) {
		RedoEntry entry = event.entry();
		if (entry == null) {
			return Map.of();
		}
		return Map.of(
				"scn", Long.toString(entry.scn()),
				"rowNumber", Integer.toString(entry.rowNumber()),
				"table", String.valueOf(entry.tableName()),
				"operation", String.valueOf(entry.operation()));
	}

	private static class NonRetryableTransformHandledException extends RuntimeException {

		NonRetryableTransformHandledException(Throwable cause) {
			super(cause);
		}
	}

}
