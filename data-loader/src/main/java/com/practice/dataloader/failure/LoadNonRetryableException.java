package com.practice.dataloader.failure;

import java.util.Map;

public class LoadNonRetryableException extends RuntimeException {

	private final FailureType failureType;
	private final String reason;
	private final Map<String, String> context;

	public LoadNonRetryableException(FailureType failureType, String reason, Map<String, String> context) {
		super(reason);
		this.failureType = failureType;
		this.reason = reason;
		this.context = context == null ? Map.of() : Map.copyOf(context);
	}

	public FailureType failureType() {
		return failureType;
	}

	public String reason() {
		return reason;
	}

	public Map<String, String> context() {
		return context;
	}

}
