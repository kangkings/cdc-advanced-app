package com.practice.dataloader.model;

public record CheckResult(
		boolean valid,
		boolean supported,
		boolean rowExists,
		Long rowId,
		String reason) {
}
