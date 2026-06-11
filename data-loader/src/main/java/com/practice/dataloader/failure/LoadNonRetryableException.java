package com.practice.dataloader.failure;

import java.util.Map;

// DLQ로 분리할 data-loader 비재시도 예외
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

	// 실패 유형 조회
	public FailureType failureType() {
		return failureType;
	}

	// DLQ에 남길 한글 사유 조회
	public String reason() {
		return reason;
	}

	// DLQ 분석용 부가 정보 조회
	public Map<String, String> context() {
		return context;
	}

}
