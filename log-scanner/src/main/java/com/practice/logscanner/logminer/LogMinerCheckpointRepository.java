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
		return load().lastScn();
	}

	// 체크포인트 테이블에서 마지막 이벤트 위치 조회
	public Checkpoint load() {
		List<Checkpoint> result = jdbcTemplate.query("""
				SELECT last_scn, last_rs_id, last_ssn
				FROM log_scanner_checkpoint
				WHERE id = ?
				""",
				(rs, rowNum) -> new Checkpoint(
						rs.getLong("last_scn"),
						rs.getString("last_rs_id"),
						nullableLong(rs, "last_ssn")),
				CHECKPOINT_ID);

		if (!result.isEmpty()) {
			Checkpoint checkpoint = result.get(0);
			log.info("[CHECKPOINT] 저장된 위치 로드. lastScn={}, lastRsId={}, lastSsn={}",
					checkpoint.lastScn(),
					checkpoint.lastRsId(),
					checkpoint.lastSsn());
			return checkpoint;
		}
		return Checkpoint.empty();
	}

	// 마지막 처리 이벤트 위치 저장
	public void save(Checkpoint checkpoint) {
		jdbcTemplate.update("""
				MERGE INTO log_scanner_checkpoint dst
				USING (SELECT ? AS id, ? AS last_scn, ? AS last_rs_id, ? AS last_ssn, SYSTIMESTAMP AS updated_at FROM dual) src
				ON (dst.id = src.id)
				WHEN MATCHED THEN
				  UPDATE SET dst.last_scn = src.last_scn,
				             dst.last_rs_id = src.last_rs_id,
				             dst.last_ssn = src.last_ssn,
				             dst.updated_at = src.updated_at
				WHEN NOT MATCHED THEN
				  INSERT (id, last_scn, last_rs_id, last_ssn, updated_at)
				  VALUES (src.id, src.last_scn, src.last_rs_id, src.last_ssn, src.updated_at)
				""",
				CHECKPOINT_ID,
				checkpoint.lastScn(),
				checkpoint.lastRsId(),
				checkpoint.lastSsn());

		log.info("[CHECKPOINT] 위치 저장 완료. lastScn={}, lastRsId={}, lastSsn={}",
				checkpoint.lastScn(),
				checkpoint.lastRsId(),
				checkpoint.lastSsn());
	}

	/**
	 * 마지막 처리 SCN 저장 (MERGE).
	 */
	public void saveLastScn(long lastScn) {
		save(new Checkpoint(lastScn, null, null));
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
					    last_rs_id VARCHAR2(64),
					    last_ssn   NUMBER,
					    updated_at TIMESTAMP NOT NULL
					)
					""");
			log.info("[CHECKPOINT] log_scanner_checkpoint 테이블 생성 완료.");
		}
		ensureColumn("LAST_RS_ID", "ALTER TABLE log_scanner_checkpoint ADD last_rs_id VARCHAR2(64)");
		ensureColumn("LAST_SSN", "ALTER TABLE log_scanner_checkpoint ADD last_ssn NUMBER");
	}

	private void ensureColumn(String columnName, String ddl) {
		Integer count = jdbcTemplate.queryForObject("""
				SELECT COUNT(*)
				FROM user_tab_columns
				WHERE table_name = 'LOG_SCANNER_CHECKPOINT'
				AND column_name = ?
				""",
				Integer.class,
				columnName);
		if (count != null && count == 0) {
			jdbcTemplate.execute(ddl);
			log.info("[CHECKPOINT] {} 컬럼 추가 완료.", columnName);
		}
	}

	private Long nullableLong(ResultSet resultSet, String column) throws SQLException {
		long value = resultSet.getLong(column);
		if (resultSet.wasNull()) {
			return null;
		}
		return value;
	}

	// LogMiner 재시작 기준 이벤트 위치
	public record Checkpoint(long lastScn, String lastRsId, Long lastSsn) {

		public static Checkpoint empty() {
			return new Checkpoint(-1L, null, null);
		}
	}

}
