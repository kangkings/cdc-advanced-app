package com.practice.datatransformer.config;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import com.practice.datatransformer.failure.FailureEvent;
import com.practice.datatransformer.failure.FailureEventProducer;
import com.practice.datatransformer.failure.FailureType;
import com.practice.datatransformer.observability.TransformerMetrics;

import io.micrometer.core.instrument.MeterRegistry;

@EnableKafka
@Configuration
// data-transformer Kafka 연결과 listener/producer Bean 구성
public class KafkaConfig {

	private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

	@Bean
	// Kafka cluster 상태 조회용 AdminClient 생성
	public AdminClient kafkaAdminClient(KafkaProperties kafkaProperties) {
		return AdminClient.create(kafkaProperties.buildAdminProperties());
	}

	@Bean
	// 부팅 시 Kafka 연결 상태 로그 출력
	public ApplicationRunner kafkaConnectionRunner(AdminClient kafkaAdminClient) {
		return args -> {
			DescribeClusterResult cluster = kafkaAdminClient.describeCluster();
			log.info("Kafka connected. clusterId={}, controller={}, nodes={}",
					cluster.clusterId().get(10, TimeUnit.SECONDS),
					cluster.controller().get(10, TimeUnit.SECONDS),
					cluster.nodes().get(10, TimeUnit.SECONDS));
		};
	}

	@Bean
	// 문자열 key/value Kafka producer factory 생성
	public ProducerFactory<String, String> producerFactory(KafkaProperties kafkaProperties) {
		Map<String, Object> properties = kafkaProperties.buildProducerProperties();
		properties.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		properties.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return new DefaultKafkaProducerFactory<>(properties);
	}

	@Bean
	// 변환 완료 이벤트 발행용 KafkaTemplate 생성
	public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
		return new KafkaTemplate<>(producerFactory);
	}

	@Bean
	// 문자열 key/value Kafka consumer factory 생성
	public ConsumerFactory<String, String> consumerFactory(KafkaProperties kafkaProperties) {
		Map<String, Object> properties = kafkaProperties.buildConsumerProperties();
		properties.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		return new DefaultKafkaConsumerFactory<>(properties);
	}

	@Bean
	// redo log entry 단건 consume listener factory 생성
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
			ConsumerFactory<String, String> consumerFactory,
			DefaultErrorHandler defaultErrorHandler) {
		ConcurrentKafkaListenerContainerFactory<String, String> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory);
		factory.setCommonErrorHandler(defaultErrorHandler);
		return factory;
	}

	@Bean
	// 설정 기반 exponential backoff error handler 생성
	public DefaultErrorHandler defaultErrorHandler(
			RetryProperties retryProperties,
			ConsumerRecordRecoverer retryExhaustedRecoverer) {
		ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(
				Math.max(0, retryProperties.maxAttempts() - 1));
		backOff.setInitialInterval(retryProperties.initialIntervalMs());
		backOff.setMultiplier(retryProperties.multiplier());
		backOff.setMaxInterval(retryProperties.maxIntervalMs());

		DefaultErrorHandler errorHandler = new DefaultErrorHandler(retryExhaustedRecoverer, backOff);
		errorHandler.addNotRetryableExceptions(OracleObjectAccessException.class);
		errorHandler.setAckAfterHandle(true);
		return errorHandler;
	}

	@Bean
	// 재시도 소진 record를 DLQ로 넘기는 recoverer 생성
	public ConsumerRecordRecoverer retryExhaustedRecoverer(
			RetryProperties retryProperties,
			FailureEventProducer failureEventProducer,
			MeterRegistry meterRegistry) {
		return (record, exception) -> publishFailure(record, exception, retryProperties, failureEventProducer, meterRegistry);
	}

	// listener 예외를 DLQ로 격리하고 offset commit 허용
	private void publishFailure(
			ConsumerRecord<?, ?> record,
			Exception exception,
			RetryProperties retryProperties,
			FailureEventProducer failureEventProducer,
			MeterRegistry meterRegistry) {
		try {
			Throwable root = rootCause(exception);
			FailureType failureType = failureType(root);
			boolean retryable = failureType == FailureType.RETRY_EXHAUSTED;
			FailureEvent failureEvent = FailureEvent.of(
					failureType,
					retryable,
					reason(failureType),
					record.topic(),
					record.partition(),
					record.offset(),
					String.valueOf(record.value()),
					failureContext(record, root, retryProperties));
			failureEventProducer.publish(failureEvent);
			meterRegistry.counter(
					TransformerMetrics.Names.FAILURE_COUNT,
					TransformerMetrics.Tags.MODULE, TransformerMetrics.MODULE,
					TransformerMetrics.Tags.FAILURE_TYPE, failureType.name(),
					TransformerMetrics.Tags.RETRYABLE, Boolean.toString(retryable))
					.increment();
			log.warn("[TRANSFORM][ERROR-HANDLER][DLQ] failureType={}, topic={}, partition={}, offset={}, exceptionType={}",
					failureType,
					record.topic(),
					record.partition(),
					record.offset(),
					root.getClass().getName());
		}
		catch (Exception publishException) {
			throw new IllegalStateException("transformer DLQ 발행에 실패했습니다", publishException);
		}
	}

	private FailureType failureType(Throwable root) {
		if (oracleObjectAccessFailure(root)) {
			return FailureType.ORACLE_OBJECT_ACCESS_FAILED;
		}
		return FailureType.RETRY_EXHAUSTED;
	}

	private String reason(FailureType failureType) {
		return switch (failureType) {
			case ORACLE_OBJECT_ACCESS_FAILED -> "Oracle 객체 또는 권한 문제로 source row 조회에 실패했습니다";
			case RETRY_EXHAUSTED -> "재시도 횟수를 모두 소진했습니다";
			default -> "변환 처리 중 예외가 발생했습니다";
		};
	}

	private boolean oracleObjectAccessFailure(Throwable throwable) {
		Throwable current = throwable;
		while (current != null) {
			if (current instanceof SQLException sqlException
					&& (sqlException.getErrorCode() == 942 || sqlException.getErrorCode() == 1031)) {
				return true;
			}
			String message = current.getMessage();
			if (message != null && (message.contains("ORA-00942") || message.contains("ORA-01031"))) {
				return true;
			}
			current = current.getCause();
		}
		return false;
	}

	private Throwable rootCause(Throwable exception) {
		Throwable current = exception;
		while (current.getCause() != null) {
			current = current.getCause();
		}
		return oracleObjectAccessFailure(exception) ? new OracleObjectAccessException(current) : current;
	}

	private Map<String, String> failureContext(
			ConsumerRecord<?, ?> record,
			Throwable exception,
			RetryProperties retryProperties) {
		Map<String, String> context = new HashMap<>();
		context.put("topic", record.topic());
		context.put("partition", String.valueOf(record.partition()));
		context.put("offset", String.valueOf(record.offset()));
		context.put("exceptionType", exception.getClass().getName());
		context.put("exceptionMessage", exception.getMessage() == null ? "" : exception.getMessage());
		context.put("maxAttempts", String.valueOf(retryProperties.maxAttempts()));
		context.put("initialIntervalMs", String.valueOf(retryProperties.initialIntervalMs()));
		context.put("multiplier", String.valueOf(retryProperties.multiplier()));
		context.put("maxIntervalMs", String.valueOf(retryProperties.maxIntervalMs()));
		return context;
	}

	private static class OracleObjectAccessException extends RuntimeException {

		OracleObjectAccessException(Throwable cause) {
			super(cause);
		}
	}

}
