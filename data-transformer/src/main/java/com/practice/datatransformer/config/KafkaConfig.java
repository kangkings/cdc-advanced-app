package com.practice.datatransformer.config;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
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
			ConsumerFactory<String, String> consumerFactory) {
		ConcurrentKafkaListenerContainerFactory<String, String> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory);
		return factory;
	}

}
