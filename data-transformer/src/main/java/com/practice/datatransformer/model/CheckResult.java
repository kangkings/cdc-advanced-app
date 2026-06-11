package com.practice.datatransformer.model;

// source row 검증과 lookup 결과
public record CheckResult(
		boolean valid,
		boolean supported,
		boolean rowExists,
		Long sourceKeyValue,
		String reason) {

	public static CheckResult valid(boolean rowExists, Long sourceKeyValue) {
		return new CheckResult(true, true, rowExists, sourceKeyValue, null);
	}

	public static CheckResult unsupported(String reason) {
		return new CheckResult(false, false, false, null, reason);
	}

	public static CheckResult invalid(String reason) {
		return new CheckResult(false, true, false, null, reason);
	}

}
