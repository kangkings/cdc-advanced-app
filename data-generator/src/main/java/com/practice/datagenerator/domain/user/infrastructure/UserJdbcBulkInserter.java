package com.practice.datagenerator.domain.user.infrastructure;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.practice.datagenerator.domain.user.domain.entity.User;
import com.practice.datagenerator.domain.user.domain.entity.UserStatus;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class UserJdbcBulkInserter {

	private final JdbcTemplate jdbcTemplate;

	@Transactional
	public void insertUsers(int count) {
		List<User> users = createUsers(count);

		jdbcTemplate.batchUpdate("""
				INSERT INTO p_users (
					name,
					email,
					status,
					created_at,
					updated_at
				)
				VALUES (?, ?, ?, ?, ?)
				""",
				users,
				users.size(),
				(statement, user) -> {
					statement.setString(1, user.name());
					statement.setString(2, user.email());
					statement.setString(3, user.status().name());
					statement.setTimestamp(4, Timestamp.valueOf(user.createdAt()));
					statement.setTimestamp(5, Timestamp.valueOf(user.updatedAt()));
				});
	}

	private List<User> createUsers(int count) {
		List<User> users = new ArrayList<>(count);
		LocalDateTime now = LocalDateTime.now();

		for (int index = 1; index <= count; index++) {
			String suffix = UUID.randomUUID().toString().substring(0, 8);
			users.add(new User(
					"CDC Rate User " + index + " " + suffix,
					"cdc-rate-user-" + index + "-" + suffix + "@example.com",
					UserStatus.ACTIVE,
					now,
					now));
		}

		return users;
	}
}
