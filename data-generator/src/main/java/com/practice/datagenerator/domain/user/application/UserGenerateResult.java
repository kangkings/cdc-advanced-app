package com.practice.datagenerator.domain.user.application;

public record UserGenerateResult(
		long jobExecutionId,
		String status,
		int count
) {
}
