package com.practice.logscanner.logminer;

import java.util.List;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.practice.logscanner.batch.model.RedoLogEntry;
import com.practice.logscanner.observability.LogScannerMetrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@Repository
// COMMIT 전 DML을 DB에 보류하고 transaction control row에서 확정/폐기
public class PendingTransactionRepository {

	private final JdbcTemplate jdbcTemplate;
	private final ObjectMapper objectMapper;
	private final MeterRegistry meterRegistry;

	public PendingTransactionRepository(DataSource dataSource, ObjectMapper objectMapper, MeterRegistry meterRegistry) {
		this.jdbcTemplate = new JdbcTemplate(dataSource);
		this.objectMapper = objectMapper;
		this.meterRegistry = meterRegistry;
		ensureTables();
	}

	// COMMIT 전 DML event DB 보류
	public void save(LogMinerRow row) {
		Timer.Sample sample = Timer.start(meterRegistry);
		try {
			RedoLogEntry entry = row.toEntry();
			mergeTransaction(row);
			jdbcTemplate.update("""
					MERGE INTO pending_logminer_events dst
					USING (
					  SELECT ? AS xid, ? AS scn, ? AS rs_id, ? AS ssn, ? AS event_json, SYSTIMESTAMP AS created_at FROM dual
					) src
					ON (
					  dst.xid = src.xid
					  AND dst.scn = src.scn
					  AND dst.rs_id = src.rs_id
					  AND dst.ssn = src.ssn
					)
					WHEN NOT MATCHED THEN
					  INSERT (xid, scn, rs_id, ssn, event_json, created_at)
					  VALUES (src.xid, src.scn, src.rs_id, src.ssn, src.event_json, src.created_at)
					""",
					row.xid(),
					row.scn(),
					normalize(row.rsId()),
					row.ssn() == null ? -1L : row.ssn(),
					writeJson(entry));
			countPendingEvents("SAVED", 1);
		}
		finally {
			recordDuration(sample, LogScannerMetrics.Names.PENDING_TRANSACTION_SAVE_DURATION);
		}
	}

	// pending transaction 존재 여부 확인
	public boolean exists(String xid) {
		Integer count = jdbcTemplate.queryForObject(
				"SELECT COUNT(*) FROM pending_logminer_transactions WHERE xid = ?",
				Integer.class,
				xid);
		return count != null && count > 0;
	}

	// COMMIT transaction event 조회 후 발행 대상으로 반환
	public List<RedoLogEntry> commit(String xid) {
		Timer.Sample sample = Timer.start(meterRegistry);
		long startedAt = System.nanoTime();
		try {
			List<RedoLogEntry> entries = findEvents(xid);
			deleteEvents(xid);
			deleteTransaction(xid);
			countPendingEvents("COMMITTED", entries.size());
			if (!entries.isEmpty()) {
				log.info("[LOGMINER][PENDING][COMMIT] xid={}, eventCount={}, elapsedMs={}",
						xid,
						entries.size(),
						elapsedMillis(startedAt));
			}
			return entries;
		}
		finally {
			recordDuration(sample, LogScannerMetrics.Names.PENDING_TRANSACTION_COMMIT_DURATION);
		}
	}

	// ROLLBACK transaction event 발행 없이 폐기
	public int rollback(String xid) {
		Timer.Sample sample = Timer.start(meterRegistry);
		long startedAt = System.nanoTime();
		try {
			int eventCount = countEvents(xid);
			deleteEvents(xid);
			deleteTransaction(xid);
			countPendingEvents("ROLLED_BACK", eventCount);
			if (eventCount > 0) {
				log.info("[LOGMINER][PENDING][ROLLBACK] xid={}, eventCount={}, elapsedMs={}",
						xid,
						eventCount,
						elapsedMillis(startedAt));
			}
			return eventCount;
		}
		finally {
			recordDuration(sample, LogScannerMetrics.Names.PENDING_TRANSACTION_ROLLBACK_DURATION);
		}
	}

	// transaction header row 생성 또는 최근 SCN 갱신
	private void mergeTransaction(LogMinerRow row) {
		jdbcTemplate.update("""
				MERGE INTO pending_logminer_transactions dst
				USING (
				  SELECT ? AS xid, ? AS xidusn, ? AS xidslt, ? AS xidsqn, ? AS first_scn, ? AS last_scn, SYSTIMESTAMP AS updated_at FROM dual
				) src
				ON (dst.xid = src.xid)
				WHEN MATCHED THEN
				  UPDATE SET dst.last_scn = src.last_scn, dst.updated_at = src.updated_at
				WHEN NOT MATCHED THEN
				  INSERT (xid, xidusn, xidslt, xidsqn, first_scn, last_scn, created_at, updated_at)
				  VALUES (src.xid, src.xidusn, src.xidslt, src.xidsqn, src.first_scn, src.last_scn, src.updated_at, src.updated_at)
				""",
				row.xid(),
				row.xidusn(),
				row.xidslt(),
				row.xidsqn(),
				row.scn(),
				row.scn());
	}

	// COMMIT 대상 pending event 조회
	private List<RedoLogEntry> findEvents(String xid) {
		Timer.Sample sample = Timer.start(meterRegistry);
		try {
			return jdbcTemplate.query("""
					SELECT event_json
					FROM pending_logminer_events
					WHERE xid = ?
					ORDER BY scn, rs_id, ssn
					""",
					(rs, rowNum) -> readJson(rs.getString("event_json")),
					xid);
		}
		finally {
			recordDuration(sample, LogScannerMetrics.Names.PENDING_TRANSACTION_FIND_DURATION);
		}
	}

	private int countEvents(String xid) {
		Integer count = jdbcTemplate.queryForObject(
				"SELECT COUNT(*) FROM pending_logminer_events WHERE xid = ?",
				Integer.class,
				xid);
		return count == null ? 0 : count;
	}

	// pending event 삭제 시간 측정
	private void deleteEvents(String xid) {
		recordDelete(() -> jdbcTemplate.update("DELETE FROM pending_logminer_events WHERE xid = ?", xid));
	}

	// pending transaction header 삭제 시간 측정
	private void deleteTransaction(String xid) {
		recordDelete(() -> jdbcTemplate.update("DELETE FROM pending_logminer_transactions WHERE xid = ?", xid));
	}

	private String writeJson(RedoLogEntry entry) {
		try {
			return objectMapper.writeValueAsString(entry);
		}
		catch (Exception ex) {
			throw new IllegalStateException("pending transaction event JSON 직렬화 실패", ex);
		}
	}

	private RedoLogEntry readJson(String json) {
		try {
			return objectMapper.readValue(json, RedoLogEntry.class);
		}
		catch (Exception ex) {
			throw new IllegalStateException("pending transaction event JSON 역직렬화 실패", ex);
		}
	}

	private String normalize(String value) {
		if (value == null) {
			return "";
		}
		return value.trim();
	}

	private void ensureTables() {
		if (!tableExists("PENDING_LOGMINER_TRANSACTIONS")) {
			jdbcTemplate.execute("""
					CREATE TABLE pending_logminer_transactions (
					    xid        VARCHAR2(32) PRIMARY KEY,
					    xidusn     NUMBER,
					    xidslt     NUMBER,
					    xidsqn     NUMBER,
					    first_scn  NUMBER NOT NULL,
					    last_scn   NUMBER NOT NULL,
					    created_at TIMESTAMP NOT NULL,
					    updated_at TIMESTAMP NOT NULL
					)
					""");
			log.info("[LOGMINER][PENDING] pending_logminer_transactions 테이블 생성 완료.");
		}
		if (!tableExists("PENDING_LOGMINER_EVENTS")) {
			jdbcTemplate.execute("""
					CREATE TABLE pending_logminer_events (
					    xid        VARCHAR2(32) NOT NULL,
					    scn        NUMBER NOT NULL,
					    rs_id      VARCHAR2(64) NOT NULL,
					    ssn        NUMBER NOT NULL,
					    event_json CLOB NOT NULL,
					    created_at TIMESTAMP NOT NULL,
					    CONSTRAINT pk_pending_logminer_events PRIMARY KEY (xid, scn, rs_id, ssn)
					)
					""");
			log.info("[LOGMINER][PENDING] pending_logminer_events 테이블 생성 완료.");
		}
	}

	private boolean tableExists(String tableName) {
		Integer count = jdbcTemplate.queryForObject(
				"SELECT COUNT(*) FROM user_tables WHERE table_name = ?",
				Integer.class,
				tableName);
		return count != null && count > 0;
	}

	// pending event 상태별 count metric 누적
	private void countPendingEvents(String status, int count) {
		if (count <= 0) {
			return;
		}
		meterRegistry.counter(
				LogScannerMetrics.Names.PENDING_TRANSACTION_EVENT_COUNT,
				LogScannerMetrics.Tags.MODULE, LogScannerMetrics.MODULE,
				LogScannerMetrics.Tags.STATUS, status).increment(count);
	}

	// delete 계열 DB 작업 duration metric 기록
	private void recordDelete(Runnable action) {
		Timer.Sample sample = Timer.start(meterRegistry);
		try {
			action.run();
		}
		finally {
			recordDuration(sample, LogScannerMetrics.Names.PENDING_TRANSACTION_DELETE_DURATION);
		}
	}

	// pending DB 작업 duration metric 기록
	private void recordDuration(Timer.Sample sample, String metricName) {
		sample.stop(Timer.builder(metricName)
				.tag(LogScannerMetrics.Tags.MODULE, LogScannerMetrics.MODULE)
				.register(meterRegistry));
	}

	// 요약 로그용 경과 시간 계산
	private long elapsedMillis(long startedAt) {
		return (System.nanoTime() - startedAt) / 1_000_000;
	}

}
