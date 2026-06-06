package com.practice.datatransformer.model;

public record CheckResult(
		boolean valid,
		boolean supported,
		boolean rowExists,
		Long rowId,
		String reason) {

	public static CheckResult valid(boolean rowExists, Long rowId) {
		return new CheckResult(true, true, rowExists, rowId, null);
	}

	public static CheckResult unsupported(String reason) {
		return new CheckResult(false, false, false, null, reason);
	}

	public static CheckResult invalid(String reason) {
		return new CheckResult(false, true, false, null, reason);
	}

}
