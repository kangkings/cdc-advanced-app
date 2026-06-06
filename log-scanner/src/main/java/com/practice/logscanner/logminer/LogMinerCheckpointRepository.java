package com.practice.logscanner.logminer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class LogMinerCheckpointRepository {

	private static final int CHECKPOINT_ID = 1;

	private final JdbcTemplate jdbcTemplate;

	public LogMinerCheckpointRepository(DataSource dataSource) {
		this.jdbcTemplate = new JdbcTemplate(dataSource);
		ensureCheckpointTable();
	}

	/**
	 * 체크포인트 테이블에서 마지막 SCN 조회.
	 * 없으면 -1 반환 (첫 실행 판단용).
	 */
	public long loadLastScn() {
		List<Long> result = jdbcTemplate.queryForList(
				"SELECT last_scn FROM log_scanner_checkpoint WHERE id = ?",
				Long.class,
				CHECKPOINT_ID);

		if (!result.isEmpty()) {
			long lastScn = result.get(0);
			log.info("[CHECKPOINT] 저장된 SCN 로드. lastScn={}", lastScn);
			return lastScn;
		}
		return -1L;
	}

	/**
	 * 마지막 처리 SCN 저장 (MERGE).
	 */
	public void saveLastScn(long lastScn) {
		jdbcTemplate.update("""
				MERGE INTO log_scanner_checkpoint dst
				USING (SELECT ? AS id, ? AS last_scn, SYSTIMESTAMP AS updated_at FROM dual) src
				ON (dst.id = src.id)
				WHEN MATCHED THEN
				  UPDATE SET dst.last_scn = src.last_scn, dst.updated_at = src.updated_at
				WHEN NOT MATCHED THEN
				  INSERT (id, last_scn, updated_at) VALUES (src.id, src.last_scn, src.updated_at)
				""", CHECKPOINT_ID, lastScn);

		log.info("[CHECKPOINT] SCN 저장 완료. lastScn={}", lastScn);
	}

	/**
	 * 현재 Oracle SCN 조회 (LogMiner 전용 연결 사용).
	 */
	public long loadCurrentScn(Connection logMinerConnection) throws SQLException {
		try (Statement statement = logMinerConnection.createStatement();
				ResultSet rs = statement.executeQuery("SELECT current_scn FROM v$database")) {
			if (rs.next()) {
				return rs.getLong("current_scn");
			}
			throw new IllegalStateException("현재 SCN을 조회할 수 없습니다.");
		}
	}

	private void ensureCheckpointTable() {
		Integer count = jdbcTemplate.queryForObject(
				"SELECT COUNT(*) FROM user_tables WHERE table_name = 'LOG_SCANNER_CHECKPOINT'",
				Integer.class);

		if (count != null && count == 0) {
			jdbcTemplate.execute("""
					CREATE TABLE log_scanner_checkpoint (
					    id         NUMBER PRIMARY KEY,
					    last_scn   NUMBER NOT NULL,
					    updated_at TIMESTAMP NOT NULL
					)
					""");
			log.info("[CHECKPOINT] log_scanner_checkpoint 테이블 생성 완료.");
		}
	}

}
