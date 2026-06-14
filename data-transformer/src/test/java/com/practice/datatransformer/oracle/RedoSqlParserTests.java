package com.practice.datatransformer.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Timestamp;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.practice.datatransformer.model.RedoEntry;
import com.practice.datatransformer.model.RowPayload;

class RedoSqlParserTests {

	private final RedoSqlParser parser = new RedoSqlParser();
	private final SourceTableMapping rule = new SourceTableMapping(
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
				1,
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
						""",
				"09000500EA080000",
				9L,
				5L,
				2282L,
				"0x000230.00001006.0010",
				29L);

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

	@Test
	void parsePayloadReadsLogMinerUpdateSetClause() {
		RedoEntry entry = new RedoEntry(
				3,
				2692700L,
				Timestamp.valueOf("2026-06-07 03:22:02"),
				"UPDATE",
				2,
				"DATA_GENERATOR",
				"P_USERS",
				"DATA_GENERATOR",
				"AAAR1rAAYAAABq6AA4",
				"""
						update "DATA_GENERATOR"."P_USERS"
						   set "STATUS" = 'INACTIVE',
						       "UPDATED_AT" = '26/06/07 03:22:02.108861'
						 where "ID" = 213387;
						""",
				"09000500EA080000",
				9L,
				5L,
				2282L,
				"0x000230.00001007.0010",
				30L);

		RowPayload payload = parser.parsePayload(entry, rule, 213387L);

		assertThat(payload.operation()).isEqualTo("UPDATE");
		assertThat(payload.key()).containsEntry("ID", 213387L);
		assertThat(payload.data())
				.containsEntry("STATUS", "INACTIVE")
				.containsEntry("UPDATED_AT", "2026-06-07T03:22:02.108861");
	}

	@Test
	void extractKeyReadsDeleteWhereClause() {
		String sqlRedo = """
				delete from "DATA_GENERATOR"."P_USERS"
				 where "ID" = 213387;
				""";

		assertThat(parser.extractKey(sqlRedo, "ID")).contains(213387L);
	}

}
