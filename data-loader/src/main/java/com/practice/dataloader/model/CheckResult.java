package com.practice.dataloader.model;

// transformer 검증 결과 전달값
public record CheckResult(
		boolean valid,
		boolean supported,
		boolean rowExists,
		Long sourceKeyValue,
		String reason) {
}
