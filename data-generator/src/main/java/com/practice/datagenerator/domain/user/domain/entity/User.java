package com.practice.datagenerator.domain.user.domain.entity;

import java.time.LocalDateTime;

// Oracle에 생성할 User 도메인 데이터
public record User(
		String name,
		String email,
		UserStatus status,
		LocalDateTime createdAt,
		LocalDateTime updatedAt
) {

	// 현재 시각 기준 User 생성
	public static User create(String name, String email, UserStatus status) {
		LocalDateTime now = LocalDateTime.now();
		return new User(name, email, status, now, now);
	}

}
