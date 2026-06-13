package com.practice.logscanner.batch.reader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.ItemStream;
import org.springframework.batch.infrastructure.item.ItemStreamException;

import com.practice.logscanner.batch.model.RedoLogEntry;
import com.practice.logscanner.logminer.LogMinerCheckpointRepository;
import com.practice.logscanner.logminer.LogMinerConnectionFactory;
import com.practice.logscanner.logminer.LogMinerTargetProperties;
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
	private List<RedoLogFile> registeredRedoLogFiles = List.of();
	private boolean logMinerStarted;
	private int transactionDiagnosticLogCount;

	public RedoLogItemReader(
			LogMinerConnectionFactory logMinerConnectionFactory,
			LogMinerCheckpointRepository checkpointRepository,
			RedoLogFileRegistrar redoLogFileRegistrar,
			LogMinerTargetProperties logMinerTargetProperties,
			MeterRegistry meterRegistry) {
		this.logMinerConnectionFactory = logMinerConnectionFactory;
		this.checkpointRepository = checkpointRepository;
		this.redoLogFileRegistrar = redoLogFileRegistrar;
		this.logMinerTargetProperties = logMinerTargetProperties;
		this.targetTables = logMinerTargetProperties.targets();
		this.meterRegistry = meterRegistry;
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		try {
			connection = logMinerConnectionFactory.getConnection();
			connection.setAutoCommit(false);

			long currentScn = checkpointRepository.loadCurrentScn(connection);
			long lastScn = checkpointRepository.loadLastScn();

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

			log.debug("[REDO-LOG-PRINT][READER][OPEN] startScn={}, endScn={}, currentScn={}, targets={}, registeredRedoLogCount={}",
					startScn,
					endScn,
					currentScn,
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
		if (contentResultSet == null) {
			return null;
		}

		while (contentResultSet.next()) {
			rowNumber++;
			logTransactionDiagnostic(contentResultSet);
			if (!isDmlOperation(contentResultSet.getString("operation"))) {
				continue;
			}
			if (transactionDiagnosticsOnly()) {
				continue;
			}
			return new RedoLogEntry(
					rowNumber,
					contentResultSet.getLong("scn"),
					contentResultSet.getTimestamp("timestamp"),
					contentResultSet.getString("operation"),
					contentResultSet.getString("seg_owner"),
					contentResultSet.getString("table_name"),
					contentResultSet.getString("username"),
					contentResultSet.getString("row_id"),
					contentResultSet.getString("sql_redo"));
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

		if (logMinerStarted) {
			endLogMinerQuietly(connectionToClose);
		}

		// LogMiner 시작 성공 후 정상 close 시점의 체크포인트 갱신
		if (logMinerStarted && endScn > startScn) {
			try {
				checkpointRepository.saveLastScn(endScn);
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
			if (rowNumber > 0) {
				meterRegistry.counter(
						LogScannerMetrics.Names.REDO_LOG_READER_COUNT,
						LogScannerMetrics.Tags.MODULE, LogScannerMetrics.MODULE).increment(rowNumber);
				log.info("[REDO-LOG-PRINT][READER][CLOSE] rowCount={}, elapsedMs={}", rowNumber, elapsed.toMillis());
			}
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
				ORDER BY scn
				""".formatted(operationFilter(), tableFilter);

		return connection.prepareStatement(sql);
	}

	private String committedDataOnlyOption() {
		if (!logMinerTargetProperties.committedDataOnly()) {
			return "";
		}
		return "+ DBMS_LOGMNR.COMMITTED_DATA_ONLY";
	}

	private long resolveEndScn(long currentScn) {
		long maxScanScnRange = logMinerTargetProperties.maxScanScnRange();
		if (maxScanScnRange <= 0 || startScn == -1L) {
			return currentScn;
		}
		long limitedEndScn = startScn + maxScanScnRange;
		if (limitedEndScn < startScn) {
			return currentScn;
		}
		return Math.min(currentScn, limitedEndScn);
	}

	private String operationFilter() {
		if (!logMinerTargetProperties.transactionDiagnostics().enabled()) {
			return "'INSERT', 'UPDATE', 'DELETE'";
		}
		return "'INSERT', 'UPDATE', 'DELETE', 'COMMIT', 'ROLLBACK'";
	}

	private boolean isDmlOperation(String operation) {
		return "INSERT".equals(operation) || "UPDATE".equals(operation) || "DELETE".equals(operation);
	}

	private boolean transactionDiagnosticsOnly() {
		LogMinerTargetProperties.TransactionDiagnostics diagnostics = logMinerTargetProperties.transactionDiagnostics();
		return diagnostics.enabled() && !diagnostics.publishEnabled();
	}

	private void logTransactionDiagnostic(ResultSet resultSet) throws SQLException {
		LogMinerTargetProperties.TransactionDiagnostics diagnostics = logMinerTargetProperties.transactionDiagnostics();
		if (!diagnostics.enabled() || transactionDiagnosticLogCount >= diagnostics.maxLogs()) {
			return;
		}
		transactionDiagnosticLogCount++;
		log.info("[LOGMINER][TX-DIAG] scn={}, operation={}, operationCode={}, xid={}, xidusn={}, xidslt={}, xidsqn={}, rsId={}, ssn={}, owner={}, table={}, rowId={}",
				resultSet.getLong("scn"),
				resultSet.getString("operation"),
				resultSet.getString("operation_code"),
				resultSet.getString("xid"),
				resultSet.getString("xidusn"),
				resultSet.getString("xidslt"),
				resultSet.getString("xidsqn"),
				resultSet.getString("rs_id"),
				resultSet.getString("ssn"),
				resultSet.getString("seg_owner"),
				resultSet.getString("table_name"),
				resultSet.getString("row_id"));
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
