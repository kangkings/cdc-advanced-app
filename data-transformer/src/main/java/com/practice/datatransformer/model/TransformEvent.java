package com.practice.datatransformer.model;

import java.time.Instant;

public record TransformEvent(
		RedoEntry entry,
		CheckResult check,
		RowPayload payload,
		Instant transformedAt) {

	public static TransformEvent of(RedoEntry entry, CheckResult check, RowPayload payload) {
		return new TransformEvent(entry, check, payload, Instant.now());
	}

}
