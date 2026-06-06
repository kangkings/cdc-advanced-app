package com.practice.logscanner.batch.reader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.ItemStream;
import org.springframework.batch.infrastructure.item.ItemStreamException;

import com.practice.logscanner.batch.model.RedoLogEntry;
import com.practice.logscanner.logminer.LogMinerCheckpointRepository;
import com.practice.logscanner.logminer.LogMinerConnectionFactory;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RedoLogItemReader implements ItemReader<RedoLogEntry>, ItemStream {

	private static final String MODULE = "log-scanner";
	private static final String READER_DURATION = "log_scanner.redo_log.reader.duration";
	private static final String READER_COUNT = "log_scanner.redo_log.reader.count";

	private final LogMinerConnectionFactory logMinerConnectionFactory;
	private final LogMinerCheckpointRepository checkpointRepository;
	private final List<String> targetTables;
	private final MeterRegistry meterRegistry;

	private Connection connection;
	private PreparedStatement contentStatement;
	private ResultSet contentResultSet;
	private int rowNumber;
	private LocalDateTime startTime;
	private long startScn;
	private long endScn;

	public RedoLogItemReader(
			LogMinerConnectionFactory logMinerConnectionFactory,
			LogMinerCheckpointRepository checkpointRepository,
			List<String> targetTables,
			MeterRegistry meterRegistry) {
		this.logMinerConnectionFactory = logMinerConnectionFactory;
		this.checkpointRepository = checkpointRepository;
		this.targetTables = targetTables;
		this.meterRegistry = meterRegistry;
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		try {
			connection = logMinerConnectionFactory.getConnection();
			connection.setAutoCommit(false);

			endScn = checkpointRepository.loadCurrentScn(connection);
			long lastScn = checkpointRepository.loadLastScn();

			// 첫 실행이면 현재 SCN부터 시작
			startScn = lastScn == -1L ? endScn : lastScn;

			if (startScn >= endScn) {
				log.debug("[REDO-LOG-PRINT][READER] 변경 없음. startScn={}, endScn={}", startScn, endScn);
				return;
			}

			List<String> redoLogFiles = findRedoLogFiles(connection);
			registerRedoLogFiles(connection, redoLogFiles);
			startLogMiner(connection, startScn, endScn);
			contentStatement = prepareContentStatement(connection);
			contentResultSet = contentStatement.executeQuery();
			startTime = LocalDateTime.now();

			log.debug("[REDO-LOG-PRINT][READER][OPEN] startScn={}, endScn={}, targets={}", startScn, endScn, targetTables);
		}
		catch (SQLException ex) {
			close();
			throw new ItemStreamException("Failed to open redo log reader.", ex);
		}
	}

	@Override
	public RedoLogEntry read() throws Exception {
		if (contentResultSet == null || !contentResultSet.next()) {
			return null;
		}

		rowNumber++;
		return new RedoLogEntry(
				rowNumber,
				contentResultSet.getLong("scn"),
				contentResultSet.getTimestamp("timestamp"),
				contentResultSet.getString("operation"),
				contentResultSet.getString("seg_owner"),
				contentResultSet.getString("table_name"),
				contentResultSet.getString("username"),
				contentResultSet.getString("sql_redo"));
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

		endLogMinerQuietly(connectionToClose);

		// 체크포인트 갱신
		if (endScn > startScn) {
			try {
				checkpointRepository.saveLastScn(endScn);
			}
			catch (Exception ex) {
				log.error("[REDO-LOG-PRINT][READER] 체크포인트 저장 실패.", ex);
			}
		}

		closeQuietly(connectionToClose);

		if (startTime != null) {
			Duration elapsed = Duration.between(startTime, LocalDateTime.now());
			Timer.builder(READER_DURATION)
					.description("Redo log print item reader duration")
					.tag("module", MODULE)
					.register(meterRegistry)
					.record(elapsed);
			if (rowNumber > 0) {
				meterRegistry.counter(READER_COUNT, "module", MODULE).increment(rowNumber);
				log.info("[REDO-LOG-PRINT][READER][CLOSE] rowCount={}, elapsedMs={}", rowNumber, elapsed.toMillis());
			}
		}
	}

	private List<String> findRedoLogFiles(Connection connection) throws SQLException {
		List<String> files = new ArrayList<>();
		try (Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery("""
						SELECT member
						FROM sys.v_$logfile
						ORDER BY group#, member
						""")) {
			while (resultSet.next()) {
				files.add(resultSet.getString("member"));
			}
		}

		if (files.isEmpty()) {
			throw new IllegalStateException("No Oracle redo log files were found.");
		}

		log.info("[REDO-LOG-PRINT][READER] Redo log files found. files={}", files);
		return files;
	}

	private void registerRedoLogFiles(Connection connection, List<String> redoLogFiles) throws SQLException {
		for (int i = 0; i < redoLogFiles.size(); i++) {
			String option = i == 0 ? "DBMS_LOGMNR.NEW" : "DBMS_LOGMNR.ADDFILE";
			registerRedoLogFile(connection, redoLogFiles.get(i), option);
		}
	}

	private void registerRedoLogFile(Connection connection, String fileName, String option) throws SQLException {
		String sql = """
				BEGIN
				  DBMS_LOGMNR.ADD_LOGFILE(
				    LOGFILENAME => ?,
				    OPTIONS => %s
				  );
				END;
				""".formatted(option);

		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, fileName);
			statement.execute();
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
					             + DBMS_LOGMNR.COMMITTED_DATA_ONLY
					             + DBMS_LOGMNR.PRINT_PRETTY_SQL
					  );
					END;
					""".formatted(startScn, endScn));
		}
	}

	private PreparedStatement prepareContentStatement(Connection connection) throws SQLException {
		String tableFilter = targetTables.stream()
				.map(t -> "'" + t.toUpperCase() + "'")
				.collect(Collectors.joining(", "));

		String sql = """
				SELECT scn, timestamp, operation, seg_owner, table_name, username, sql_redo
				FROM v$logmnr_contents
				WHERE operation IN ('INSERT', 'UPDATE', 'DELETE')
				AND table_name IN (%s)
				ORDER BY scn
				""".formatted(tableFilter);

		return connection.prepareStatement(sql);
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
