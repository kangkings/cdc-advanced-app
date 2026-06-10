package com.practice.datatransformer.model;

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
