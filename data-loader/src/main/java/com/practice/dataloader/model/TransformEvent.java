package com.practice.dataloader.model;

import java.time.Instant;

// data-loader가 소비하는 변환 완료 이벤트
public record TransformEvent(
		RedoEntry entry,
		CheckResult check,
		RowPayload payload,
		Instant transformedAt) {
}
