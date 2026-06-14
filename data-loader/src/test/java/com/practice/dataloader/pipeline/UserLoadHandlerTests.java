package com.practice.dataloader.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;

class UserLoadHandlerTests {

	private final UserLoadHandler userLoadHandler = new UserLoadHandler(null, null);

	@Test
	void asLocalDateTimeParsesIsoTimestampFromTransformer() {
		LocalDateTime parsed = userLoadHandler.asLocalDateTime("2026-06-07T03:21:02.108861");

		assertThat(parsed).isEqualTo(LocalDateTime.of(2026, 6, 7, 3, 21, 2, 108861000));
	}

	@Test
	void asLocalDateTimeParsesLegacyOracleLogMinerTimestamp() {
		LocalDateTime parsed = userLoadHandler.asLocalDateTime("26/06/07 03:21:02.108861");

		assertThat(parsed).isEqualTo(LocalDateTime.of(2026, 6, 7, 3, 21, 2, 108861000));
	}

}
