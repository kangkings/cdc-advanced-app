package com.practice.datatransformer.model;

import java.time.Instant;

// data-loader로 발행할 변환 완료 이벤트
public record TransformEvent(
		RedoEntry entry,
		CheckResult check,
		RowPayload payload,
		Instant transformedAt) {

	public static TransformEvent of(RedoEntry entry, CheckResult check, RowPayload payload) {
		return new TransformEvent(entry, check, payload, Instant.now());
	}

}
