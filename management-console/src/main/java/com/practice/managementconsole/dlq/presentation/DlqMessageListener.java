package com.practice.managementconsole.dlq.presentation;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.practice.managementconsole.dlq.application.DlqMessageCollector;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
// DLQ topic 메시지를 수집해 저장하는 Kafka listener
public class DlqMessageListener {

	private final DlqMessageCollector dlqMessageCollector;

	// loader/transformer DLQ 메시지 저장 후 offset commit
	@KafkaListener(topics = {
			"${cdc.kafka.loader-dlq-topic}",
			"${cdc.kafka.transformer-dlq-topic}"
	})
	public void listen(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
		dlqMessageCollector.collect(record.value(), record.topic(), record.partition(), record.offset());
		acknowledgment.acknowledge();
		log.info("[DLQ-MESSAGE][ACK] topic={}, partition={}, offset={}",
				record.topic(),
				record.partition(),
				record.offset());
	}

}
