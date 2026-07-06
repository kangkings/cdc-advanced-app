package com.practice.managementconsole.dlq.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

// DLQ 수집과 replay topic 설정 바인딩
@ConfigurationProperties(prefix = "cdc.kafka")
public record DlqTopicProperties(
		String loaderDlqTopic,
		String transformerDlqTopic,
		String replayTopic) {
}
