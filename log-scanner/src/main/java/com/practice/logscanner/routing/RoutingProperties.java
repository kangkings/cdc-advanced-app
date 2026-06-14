package com.practice.logscanner.routing;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

// CDC aggregate routing rule 설정 바인딩
@ConfigurationProperties(prefix = "cdc.routing")
public record RoutingProperties(
		List<TableRule> tables) {

	public RoutingProperties {
		if (tables == null) {
			tables = List.of();
		}
	}

	// source table별 aggregate key 생성 규칙
	public record TableRule(
			String tableName,
			String aggregateName,
			String aggregateKeyColumn) {
	}

}
