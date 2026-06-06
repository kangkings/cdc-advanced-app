package com.practice.datagenerator.domain.user.domain.entity;

import java.time.LocalDateTime;

public record User(
		String name,
		String email,
		UserStatus status,
		LocalDateTime createdAt,
		LocalDateTime updatedAt
) {

	public static User create(String name, String email, UserStatus status) {
		LocalDateTime now = LocalDateTime.now();
		return new User(name, email, status, now, now);
	}

}
