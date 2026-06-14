package com.practice.logscanner.batch.reader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.ItemStream;
import org.springframework.batch.infrastructure.item.ItemStreamException;

import com.practice.logscanner.batch.model.RedoLogEntry;
import com.practice.logscanner.logminer.LogMinerCheckpointRepository;
import com.practice.logscanner.logminer.LogMinerCheckpointRepository.Checkpoint;
import com.practice.logscanner.logminer.LogMinerConnectionFactory;
import com.practice.logscanner.logminer.LogMinerRow;
import com.practice.logscanner.logminer.LogMinerTargetProperties;
import com.practice.logscanner.logminer.PendingTransactionRepository;
import com.practice.logscanner.logminer.RedoLogFileRegistrar;
import com.practice.logscanner.logminer.RedoLogFileRegistrar.RedoLogFile;
import com.practice.logscanner.observability.LogScannerMetrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
// LogMiner 세션에서 redo log를 chunk 단위로 읽는 reader
public class RedoLogItemReader implements ItemReader<RedoLogEntry>, ItemStream {

	private final LogMinerConnectionFactory logMinerConnectionFactory;
	private final LogMinerCheckpointRepository checkpointRepository;
	private final RedoLogFileRegistrar redoLogFileRegistrar;
	private final PendingTransactionRepository pendingTransactionRepository;
	private final LogMinerTargetProperties logMinerTargetProperties;
	private final List<String> targetTables;
	private final MeterRegistry meterRegistry;

	private Connection connection;
	private PreparedStatement contentStatement;
	private ResultSet contentResultSet;
	private int rowNumber;
	private LocalDateTime startTime;
	private long startScn;
	private long endScn;
	private long currentScn;
	private long lastReadScn = -1L;
	private String lastReadRsId;
	private Long lastReadSsn;
	private Checkpoint startCheckpoint = Checkpoint.empty();
	private long checkpointSavedScn = -1L;
	private List<RedoLogFile> registeredRedoLogFiles = List.of();
	private boolean logMinerStarted;
	private int transactionDiagnosticLogCount;
	private int emittedEntryCount;
	private int pendingSavedCount;
	private int pendingCommittedCount;
	private int pendingRolledBackCount;
	private int skippedControlCount;
	private final Deque<RedoLogEntry> publishQueue = new ArrayDeque<>();
	private final Set<String> diagnosticTargetXids = new HashSet<>();
	private final Set<String> pendingTargetXids = new HashSet<>();

	public RedoLogItemReader(
			LogMinerConnectionFactory logMinerConnectionFactory,
			LogMinerCheckpointRepository checkpointRepository,
			RedoLogFileRegistrar redoLogFileRegistrar,
			PendingTransactionRepository pendingTransactionRepository,
			LogMinerTargetProperties logMinerTargetProperties,
			MeterRegistry meterRegistry) {
		this.logMinerConnectionFactory = logMinerConnectionFactory;
		this.checkpointRepository = checkpointRepository;
		this.redoLogFileRegistrar = redoLogFileRegistrar;
		this.pendingTransactionRepository = pendingTransactionRepository;
		this.logMinerTargetProperties = logMinerTargetProperties;
		this.targetTables = logMinerTargetProperties.targets();
		this.meterRegistry = meterRegistry;
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		try {
			connection = logMinerConnectionFactory.getConnection();
			connection.setAutoCommit(false);

			currentScn = checkpointRepository.loadCurrentScn(connection);
			startCheckpoint = checkpointRepository.load();
			long lastScn = startCheckpoint.lastScn();

			// 첫 실행이면 현재 SCN부터 시작
			startScn = lastScn == -1L ? currentScn : lastScn;
			endScn = resolveEndScn(currentScn);

			if (startScn >= endScn) {
				log.debug("[REDO-LOG-PRINT][READER] 변경 없음. startScn={}, endScn={}", startScn, endScn);
				return;
			}

			registeredRedoLogFiles = redoLogFileRegistrar.registerAll(connection, startScn, endScn);
			startLogMiner(connection, startScn, endScn);
			logMinerStarted = true;
			contentStatement = prepareContentStatement(connection);
			contentResultSet = contentStatement.executeQuery();
			startTime = LocalDateTime.now();

			log.debug("[REDO-LOG-PRINT][READER][OPEN] startScn={}, endScn={}, currentScn={}, safetyMargin={}, currentScnLag={}, targets={}, registeredRedoLogCount={}",
					startScn,
					endScn,
					currentScn,
					logMinerTargetProperties.endScnSafetyMargin(),
					Math.max(0L, currentScn - endScn),
					targetTables,
					registeredRedoLogFiles.size());
		}
		catch (SQLException ex) {
			close();
			throw new ItemStreamException("Failed to open redo log reader.", ex);
		}
	}

	@Override
	public RedoLogEntry read() throws Exception {
		if (!publishQueue.isEmpty()) {
			return pollPublishedEntry();
		}
		if (contentResultSet == null) {
			return null;
		}

		while (contentResultSet.next()) {
			rowNumber++;
			LogMinerRow row = LogMinerRow.from(contentResultSet, rowNumber);
			lastReadScn = row.scn();
			lastReadRsId = row.rsId();
			lastReadSsn = row.ssn();
			logTransactionDiagnostic(row);
			if (!row.dml() && !row.commit() && !row.rollback()) {
				continue;
			}
			if (transactionDiagnosticsOnly()) {
				continue;
			}
			if (logMinerTargetProperties.committedDataOnly()) {
				if (row.dml()) {
					return emit(row.toEntry());
				}
				continue;
			}
			RedoLogEntry pendingEntry = handleTransactionAwareRow(row);
			if (pendingEntry != null) {
				return pendingEntry;
			}
		}
		return null;
	}

	@Override
	public void update(ExecutionContext executionContext) {
		executionContext.putInt("redoLogPrint.rowNumber", rowNumber);
	}

	@Override
	public void close() throws ItemStreamException {
		if (connection == null) {
			return;
		}

		Connection connectionToClose = connection;
		connection = null;

		closeQuietly(contentResultSet);
		contentResultSet = null;

		closeQuietly(contentStatement);
		contentStatement = null;
		publishQueue.clear();
		diagnosticTargetXids.clear();
		pendingTargetXids.clear();

		if (logMinerStarted) {
			endLogMinerQuietly(connectionToClose);
		}

		// LogMiner 시작 성공 후 실제 읽은 SCN 기준 체크포인트 갱신
		if (logMinerStarted && endScn > startScn) {
			try {
				Checkpoint checkpoint = resolveCheckpoint();
				checkpointSavedScn = checkpoint.lastScn();
				checkpointRepository.save(checkpoint);
				countCheckpointSave();
			}
			catch (Exception ex) {
				log.error("[REDO-LOG-PRINT][READER] 체크포인트 저장 실패.", ex);
			}
		}

		closeQuietly(connectionToClose);
		logMinerStarted = false;

		if (startTime != null) {
			Duration elapsed = Duration.between(startTime, LocalDateTime.now());
			Timer.builder(LogScannerMetrics.Names.REDO_LOG_READER_DURATION)
					.description("Redo log print item reader duration")
					.tag(LogScannerMetrics.Tags.MODULE, LogScannerMetrics.MODULE)
					.register(meterRegistry)
					.record(elapsed);
			recordScanSummary(elapsed);
		}
	}

	private void startLogMiner(Connection connection, long startScn, long endScn) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			statement.execute("""
					BEGIN
					  DBMS_LOGMNR.START_LOGMNR(
					    STARTSCN => %d,
					    ENDSCN   => %d,
					    OPTIONS  => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG
					             %s
					             + DBMS_LOGMNR.PRINT_PRETTY_SQL
					  );
					END;
					""".formatted(startScn, endScn, committedDataOnlyOption()));
		}
		catch (SQLException ex) {
			String error = ex.getErrorCode() == 1291 ? "ORA-01291" : "UNKNOWN";
			meterRegistry.counter(
					LogScannerMetrics.Names.LOGMINER_START_FAILURE_COUNT,
					LogScannerMetrics.Tags.MODULE, LogScannerMetrics.MODULE,
					LogScannerMetrics.Tags.ERROR, error).increment();
			log.error("[LOGMINER][START-FAILED] startScn={}, endScn={}, error={}, registeredCount={}, onlineCount={}, archivedCount={}, files={}",
					startScn,
					endScn,
					error,
					registeredRedoLogFiles.size(),
					registeredRedoLogFiles.stream().filter(RedoLogFile::online).count(),
					registeredRedoLogFiles.stream().filter(RedoLogFile::archived).count(),
					registeredRedoLogFiles,
					ex);
			throw ex;
		}
	}

	private PreparedStatement prepareContentStatement(Connection connection) throws SQLException {
		String tableFilter = targetTables.stream()
				.map(t -> "'" + t.toUpperCase() + "'")
				.collect(Collectors.joining(", "));

		String sql = """
				SELECT scn, timestamp, operation, operation_code, seg_owner, table_name, username,
				       row_id, sql_redo, xid, xidusn, xidslt, xidsqn, rs_id, ssn
				FROM v$logmnr_contents
				WHERE operation IN (%s)
				AND (table_name IN (%s) OR operation IN ('COMMIT', 'ROLLBACK'))
				%s
				ORDER BY scn, rs_id, ssn
				""".formatted(operationFilter(), tableFilter, resumePredicate());

		PreparedStatement statement = connection.prepareStatement(sql);
		if (hasCheckpointIdentity()) {
			statement.setLong(1, startCheckpoint.lastScn());
			statement.setLong(2, startCheckpoint.lastScn());
			statement.setString(3, startCheckpoint.lastRsId());
			statement.setString(4, startCheckpoint.lastRsId());
			statement.setLong(5, startCheckpoint.lastSsn() == null ? -1L : startCheckpoint.lastSsn());
		}
		return statement;
	}

	// checkpoint identity가 있으면 같은 SCN 내부에서 이미 읽은 row 이후부터 재개
	private String resumePredicate() {
		if (!hasCheckpointIdentity()) {
			return "";
		}
		return """
				AND (
				  scn > ?
				  OR (
				    scn = ?
				    AND (
				      rs_id > ?
				      OR (rs_id = ? AND NVL(ssn, -1) > ?)
				    )
				  )
				)
				""";
	}

	private boolean hasCheckpointIdentity() {
		return startCheckpoint.lastScn() >= 0
				&& startCheckpoint.lastRsId() != null
				&& startCheckpoint.lastSsn() != null;
	}

	private String committedDataOnlyOption() {
		if (!logMinerTargetProperties.committedDataOnly()) {
			return "";
		}
		return "+ DBMS_LOGMNR.COMMITTED_DATA_ONLY";
	}

	private RedoLogEntry handleTransactionAwareRow(LogMinerRow row) {
		if (row.dml()) {
			if (blank(row.xid())) {
				log.warn("[LOGMINER][PENDING][BYPASS] xid 없음. scn={}, row={}, operation={}, table={}",
						row.scn(),
						row.rowNumber(),
						row.operation(),
						row.tableName());
				return row.toEntry();
			}
			pendingTransactionRepository.save(row);
			pendingTargetXids.add(row.xid());
			pendingSavedCount++;
			return null;
		}
		if (row.commit()) {
			if (!shouldHandleTransactionControl(row)) {
				countSkippedTransactionControl(row);
				return null;
			}
			List<RedoLogEntry> committedEntries = pendingTransactionRepository.commit(row.xid());
			pendingCommittedCount += committedEntries.size();
			publishQueue.addAll(committedEntries);
			pendingTargetXids.remove(row.xid());
			return pollPublishedEntry();
		}
		if (row.rollback()) {
			if (!shouldHandleTransactionControl(row)) {
				countSkippedTransactionControl(row);
				return null;
			}
			pendingRolledBackCount += pendingTransactionRepository.rollback(row.xid());
			pendingTargetXids.remove(row.xid());
		}
		return null;
	}

	private boolean shouldHandleTransactionControl(LogMinerRow row) {
		if (blank(row.xid())) {
			return false;
		}
		return pendingTargetXids.contains(row.xid()) || pendingTransactionRepository.exists(row.xid());
	}

	private void countSkippedTransactionControl(LogMinerRow row) {
		skippedControlCount++;
		meterRegistry.counter(
				LogScannerMetrics.Names.PENDING_TRANSACTION_CONTROL_SKIP_COUNT,
				LogScannerMetrics.Tags.MODULE, LogScannerMetrics.MODULE,
				LogScannerMetrics.Tags.TYPE, row.operation()).increment();
	}

	// 발행 대상으로 반환한 entry 수 추적
	private RedoLogEntry emit(RedoLogEntry entry) {
		emittedEntryCount++;
		return entry;
	}

	// pending queue에서 꺼낸 entry 발행 수 추적
	private RedoLogEntry pollPublishedEntry() {
		RedoLogEntry entry = publishQueue.pollFirst();
		return entry == null ? null : emit(entry);
	}

	// scan window 종료 시 checkpoint 저장 횟수 기록
	private void countCheckpointSave() {
		meterRegistry.counter(
				LogScannerMetrics.Names.CHECKPOINT_SAVE_COUNT,
				LogScannerMetrics.Tags.MODULE, LogScannerMetrics.MODULE).increment();
	}

	// scan window 단위 raw/발행/pending 요약 metric과 로그 기록
	private void recordScanSummary(Duration elapsed) {
		meterRegistry.counter(
				LogScannerMetrics.Names.REDO_LOG_SCAN_WINDOW_COUNT,
				LogScannerMetrics.Tags.MODULE, LogScannerMetrics.MODULE).increment();
		meterRegistry.summary(
				LogScannerMetrics.Names.REDO_LOG_SCAN_WINDOW_SCN_RANGE,
				LogScannerMetrics.Tags.MODULE, LogScannerMetrics.MODULE).record(Math.max(0L, endScn - startScn));
		incrementScanEntry("RAW", rowNumber);
		incrementScanEntry("EMITTED", emittedEntryCount);
		incrementScanEntry("PENDING_SAVED", pendingSavedCount);
		incrementScanEntry("PENDING_COMMITTED", pendingCommittedCount);
		incrementScanEntry("PENDING_ROLLED_BACK", pendingRolledBackCount);
		incrementScanEntry("CONTROL_SKIPPED", skippedControlCount);

		if (rowNumber > 0) {
			meterRegistry.counter(
					LogScannerMetrics.Names.REDO_LOG_READER_COUNT,
					LogScannerMetrics.Tags.MODULE, LogScannerMetrics.MODULE).increment(rowNumber);
		}
		log.info("[REDO-LOG-PRINT][SCAN][SUMMARY] startScn={}, endScn={}, currentScn={}, safetyMargin={}, currentScnLag={}, lastReadScn={}, lastReadRsId={}, lastReadSsn={}, checkpointSavedScn={}, rawRows={}, emitted={}, pendingSaved={}, pendingCommitted={}, pendingRolledBack={}, controlSkipped={}, registeredRedoLogCount={}, elapsedMs={}",
				startScn,
				endScn,
				currentScn,
				logMinerTargetProperties.endScnSafetyMargin(),
				Math.max(0L, currentScn - endScn),
				lastReadScn,
				lastReadRsId,
				lastReadSsn,
				checkpointSavedScn,
				rowNumber,
				emittedEntryCount,
				pendingSavedCount,
				pendingCommittedCount,
				pendingRolledBackCount,
				skippedControlCount,
				registeredRedoLogFiles.size(),
				elapsed.toMillis());
	}

	// scan summary entry type별 count metric 누적
	private void incrementScanEntry(String type, int amount) {
		if (amount <= 0) {
			return;
		}
		meterRegistry.counter(
				LogScannerMetrics.Names.REDO_LOG_SCAN_ENTRY_COUNT,
				LogScannerMetrics.Tags.MODULE, LogScannerMetrics.MODULE,
				LogScannerMetrics.Tags.TYPE, type).increment(amount);
	}

	private long resolveEndScn(long currentScn) {
		long stableCurrentScn = resolveStableCurrentScn(currentScn);
		long maxScanScnRange = logMinerTargetProperties.maxScanScnRange();
		if (maxScanScnRange <= 0 || startScn == -1L) {
			return stableCurrentScn;
		}
		long limitedEndScn = startScn + maxScanScnRange;
		if (limitedEndScn < startScn) {
			return stableCurrentScn;
		}
		return Math.min(stableCurrentScn, limitedEndScn);
	}

	// current_scn 직전 safety margin을 둔 안정 스캔 종료 SCN 계산
	private long resolveStableCurrentScn(long currentScn) {
		long safetyMargin = Math.max(0L, logMinerTargetProperties.endScnSafetyMargin());
		if (safetyMargin == 0L) {
			return currentScn;
		}
		return Math.max(0L, currentScn - safetyMargin);
	}

	// raw row가 있으면 마지막으로 관측한 SCN, 없으면 빈 구간 종료 SCN 저장
	private Checkpoint resolveCheckpoint() {
		if (lastReadScn >= startScn && lastReadScn <= endScn) {
			return new Checkpoint(lastReadScn, lastReadRsId, lastReadSsn);
		}
		return new Checkpoint(endScn, null, null);
	}

	private String operationFilter() {
		if (logMinerTargetProperties.committedDataOnly() && !logMinerTargetProperties.transactionDiagnostics().enabled()) {
			return "'INSERT', 'UPDATE', 'DELETE'";
		}
		return "'INSERT', 'UPDATE', 'DELETE', 'COMMIT', 'ROLLBACK'";
	}

	private boolean transactionDiagnosticsOnly() {
		LogMinerTargetProperties.TransactionDiagnostics diagnostics = logMinerTargetProperties.transactionDiagnostics();
		return diagnostics.enabled() && !diagnostics.publishEnabled();
	}

	private void logTransactionDiagnostic(LogMinerRow row) {
		LogMinerTargetProperties.TransactionDiagnostics diagnostics = logMinerTargetProperties.transactionDiagnostics();
		if (!diagnostics.enabled() || !shouldLogTransactionDiagnostic(row, diagnostics)
				|| transactionDiagnosticLogCount >= diagnostics.maxLogs()) {
			return;
		}
		transactionDiagnosticLogCount++;
		log.info("[LOGMINER][TX-DIAG] scn={}, operation={}, operationCode={}, xid={}, xidusn={}, xidslt={}, xidsqn={}, rsId={}, ssn={}, owner={}, table={}, rowId={}",
				row.scn(),
				row.operation(),
				row.operationCode(),
				row.xid(),
				row.xidusn(),
				row.xidslt(),
				row.xidsqn(),
				row.rsId(),
				row.ssn(),
				row.owner(),
				row.tableName(),
				row.rowId());
	}

	private boolean shouldLogTransactionDiagnostic(
			LogMinerRow row,
			LogMinerTargetProperties.TransactionDiagnostics diagnostics) {
		if (row.dml() && targetTable(row.tableName())) {
			rememberDiagnosticXid(row.xid());
			return true;
		}
		if (!row.commit() && !row.rollback()) {
			return false;
		}
		if (diagnostics.logAllControls()) {
			return true;
		}
		return row.xid() != null && diagnosticTargetXids.contains(row.xid());
	}

	private void rememberDiagnosticXid(String xid) {
		if (!blank(xid)) {
			diagnosticTargetXids.add(xid);
		}
	}

	private boolean targetTable(String tableName) {
		if (tableName == null) {
			return false;
		}
		String normalizedTableName = tableName.toUpperCase(Locale.ROOT);
		return targetTables.stream()
				.map(target -> target.toUpperCase(Locale.ROOT))
				.anyMatch(normalizedTableName::equals);
	}

	private boolean blank(String value) {
		return value == null || value.isBlank();
	}

	private void endLogMinerQuietly(Connection connection) {
		try (Statement statement = connection.createStatement()) {
			statement.execute("""
					BEGIN
					  DBMS_LOGMNR.END_LOGMNR;
					END;
					""");
		}
		catch (SQLException ex) {
			log.warn("[REDO-LOG-PRINT][READER] Failed to end LogMiner session.", ex);
		}
	}

	private void closeQuietly(AutoCloseable closeable) {
		if (closeable == null) {
			return;
		}
		try {
			closeable.close();
		}
		catch (Exception ex) {
			log.warn("[REDO-LOG-PRINT][READER] Failed to close resource.", ex);
		}
	}

}
