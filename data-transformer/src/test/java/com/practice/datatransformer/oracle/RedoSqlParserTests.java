package com.practice.datatransformer.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Timestamp;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.practice.datatransformer.model.RedoEntry;
import com.practice.datatransformer.model.RowPayload;

class RedoSqlParserTests {

	private final RedoSqlParser parser = new RedoSqlParser();
	private final TableRule rule = new TableRule(
			"DATA_GENERATOR",
			"P_USERS",
			"ID",
			List.of("NAME", "EMAIL", "STATUS", "CREATED_AT", "UPDATED_AT"));

	@Test
	void parsePayloadReadsLogMinerAssignmentStyleInsert() {
		RedoEntry entry = new RedoEntry(
				2,
				2692618L,
				Timestamp.valueOf("2026-06-07 03:21:02"),
				"INSERT",
				"DATA_GENERATOR",
				"P_USERS",
				"DATA_GENERATOR",
				"AAAR1rAAYAAABq6AA3",
				"""
						insert into "DATA_GENERATOR"."P_USERS"
						 values
						    "ID" = 213387,
						    "NAME" = 'CDC User 1 eeeadea1',
						    "EMAIL" = 'cdc-user-1-eeeadea1@example.com',
						    "STATUS" = 'ACTIVE',
						    "CREATED_AT" = '26/06/07 03:21:02.108861',
						    "UPDATED_AT" = '26/06/07 03:21:02.108861';
						""");

		RowPayload payload = parser.parsePayload(entry, rule, 213387L);

		assertThat(payload.key()).containsEntry("ID", 213387L);
		assertThat(payload.data())
				.containsEntry("ID", 213387L)
				.containsEntry("NAME", "CDC User 1 eeeadea1")
				.containsEntry("EMAIL", "cdc-user-1-eeeadea1@example.com")
				.containsEntry("STATUS", "ACTIVE")
				.containsEntry("CREATED_AT", "2026-06-07T03:21:02.108861")
				.containsEntry("UPDATED_AT", "2026-06-07T03:21:02.108861");
	}

}
