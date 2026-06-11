package com.practice.dataloader.pipeline;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.practice.dataloader.failure.FailureEvent;
import com.practice.dataloader.failure.FailureEventProducer;
import com.practice.dataloader.failure.FailureType;
import com.practice.dataloader.failure.LoadNonRetryableException;
import com.practice.dataloader.model.TransformEvent;
import com.practice.dataloader.observability.LoaderMetrics;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@Component
@RequiredArgsConstructor
// transformer 이벤트 소비와 DLQ 분리 진입점
public class TransformEventListener {

	private final ObjectMapper objectMapper;
	private final LoadService loadService;
	private final FailureEventProducer failureEventProducer;
	private final MeterRegistry meterRegistry;

	@KafkaListener(
			topics = "${cdc.kafka.input-topic}",
			groupId = "${spring.kafka.consumer.group-id}",
			containerFactory = "kafkaListenerContainerFactory")
	public void listen(List<ConsumerRecord<String, String>> records) throws Exception {
		LocalDateTime startTime = LocalDateTime.now();
		String status = LoaderMetrics.Status.SUCCESS;
		try {
			List<TransformEvent> validEvents = new ArrayList<>();
			List<FailureCandidate> failureCandidates = new ArrayList<>();
			for (ConsumerRecord<String, String> record : records) {
				handleRecord(record, validEvents, failureCandidates);
			}
			if (!validEvents.isEmpty()) {
				loadService.loadBatch(validEvents);
			}
			publishFailures(failureCandidates);
		}
		catch (Exception ex) {
			status = LoaderMetrics.Status.FAILED;
			log.error("[LOAD][BATCH][FAILED] batchSize={}", records.size(), ex);
			throw ex;
		}
		finally {
			meterRegistry.counter(
					LoaderMetrics.Names.KAFKA_CONSUME_COUNT,
					LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE,
					LoaderMetrics.Tags.STATUS, status).increment(records.size());
			io.micrometer.core.instrument.Timer.builder(LoaderMetrics.Names.KAFKA_LISTENER_DURATION)
					.description("Data loader Kafka batch listener duration")
					.tag(LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE)
					.tag(LoaderMetrics.Tags.STATUS, status)
					.register(meterRegistry)
					.record(Duration.between(startTime, LocalDateTime.now()));
		}
	}

	// 원본 메시지를 검증하고 비재시도 실패는 DB 성공 이후 DLQ 후보로 보류
	private void handleRecord(
			ConsumerRecord<String, String> record,
			List<TransformEvent> validEvents,
			List<FailureCandidate> failureCandidates) {
		try {
			TransformEvent event = deserialize(record);
			loadService.validate(event);
			validEvents.add(event);
		}
		catch (LoadNonRetryableException ex) {
			failureCandidates.add(new FailureCandidate(record, ex));
		}
	}

	// Kafka 메시지 역직렬화 실패를 비재시도 실패로 변환
	private TransformEvent deserialize(ConsumerRecord<String, String> record) {
		try {
			return objectMapper.readValue(record.value(), TransformEvent.class);
		}
		catch (Exception ex) {
			throw new LoadNonRetryableException(
					FailureType.DESERIALIZATION_FAILED,
					"TransformEvent JSON을 읽을 수 없습니다",
					Map.of(
							"topic", record.topic(),
							"partition", String.valueOf(record.partition()),
							"offset", String.valueOf(record.offset())));
		}
	}

	// DB write 성공 이후 보류된 비재시도 실패 DLQ 발행
	private void publishFailures(List<FailureCandidate> failureCandidates) throws Exception {
		for (FailureCandidate candidate : failureCandidates) {
			publishFailure(candidate.record(), candidate.exception());
		}
	}

	// 비재시도 실패를 공용 FailureEvent로 만들어 DLQ 발행
	private void publishFailure(ConsumerRecord<String, String> record, LoadNonRetryableException ex) throws Exception {
		FailureEvent failureEvent = FailureEvent.of(
				ex.failureType(),
				false,
				ex.reason(),
				record.topic(),
				record.partition(),
				record.offset(),
				record.value(),
				ex.context());
		failureEventProducer.publish(failureEvent);
		meterRegistry.counter(
				LoaderMetrics.Names.FAILURE_COUNT,
				LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE,
				LoaderMetrics.Tags.FAILURE_TYPE, ex.failureType().name(),
				LoaderMetrics.Tags.RETRYABLE, "false",
				LoaderMetrics.Tags.STATUS, LoaderMetrics.Status.DLQ)
				.increment();
		log.warn("[LOAD][DLQ] failureType={}, reason={}, topic={}, partition={}, offset={}",
				ex.failureType(),
				ex.reason(),
				record.topic(),
				record.partition(),
				record.offset());
	}

	private record FailureCandidate(
			ConsumerRecord<String, String> record,
			LoadNonRetryableException exception) {
	}

}
