package com.practice.datagenerator.batch.processor;

import java.util.UUID;

import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.practice.datagenerator.domain.user.domain.entity.User;
import com.practice.datagenerator.domain.user.domain.entity.UserStatus;

@Component
public class UserGenerateItemProcessor implements ItemProcessor<Integer, User> {

	@Override
	public User process(Integer item) {
		String suffix = UUID.randomUUID().toString().substring(0, 8);
		return User.create(
				"CDC User " + item + " " + suffix,
				"cdc-user-" + item + "-" + suffix + "@example.com",
				UserStatus.ACTIVE);
	}

}
