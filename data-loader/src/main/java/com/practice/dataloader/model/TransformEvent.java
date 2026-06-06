package com.practice.dataloader.model;

import java.time.Instant;

public record TransformEvent(
		RedoEntry entry,
		CheckResult check,
		RowPayload payload,
		Instant transformedAt) {
}
