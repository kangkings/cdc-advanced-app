package com.practice.logscanner.logminer;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.practice.logscanner.observability.LogScannerMetrics;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class RedoLogFileRegistrar {

	private final MeterRegistry meterRegistry;

	public RedoLogFileRegistrar(MeterRegistry meterRegistry) {
		this.meterRegistry = meterRegistry;
	}

	public List<RedoLogFile> registerAll(Connection connection) throws SQLException {
		List<RedoLogFile> redoLogFiles = findOnlineRedoLogFiles(connection);
		List<RedoLogFile> registeredFiles = registerRedoLogFiles(connection, redoLogFiles);
		logRegisteredFiles(registeredFiles);
		return registeredFiles;
	}

	public List<RedoLogFile> registerAll(Connection connection, long startScn, long endScn) throws SQLException {
		List<RedoLogFile> onlineCandidates = findOnlineRedoLogFiles(connection);
		List<RedoLogFile> archivedCandidates = findArchivedRedoLogFiles(connection, startScn, endScn);
		List<RedoLogFile> redoLogFiles = mergeByPath(onlineCandidates, archivedCandidates);
		log.info("[LOGMINER][REDO-LOG][CANDIDATES] onlineCount={}, archivedCount={}, mergedCount={}, startScn={}, endScn={}",
				onlineCandidates.size(),
				archivedCandidates.size(),
				redoLogFiles.size(),
				startScn,
				endScn);
		List<RedoLogFile> registeredFiles = registerRedoLogFiles(connection, redoLogFiles);
		logRegisteredFiles(registeredFiles);
		return registeredFiles;
	}

	private List<RedoLogFile> findOnlineRedoLogFiles(Connection connection) throws SQLException {
		List<RedoLogFile> files = new ArrayList<>();
		try (Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery("""
						SELECT DISTINCT lf.member, l.first_change# AS first_change, l.next_change# AS next_change
						FROM sys.v_$logfile lf
						JOIN sys.v_$log l ON lf.group# = l.group#
						ORDER BY l.first_change#, lf.member
						""")) {
			while (resultSet.next()) {
				files.add(new RedoLogFile(
						resultSet.getString("member"),
						RedoLogFileType.ONLINE,
						getNullableLong(resultSet, "first_change"),
						getNullableLong(resultSet, "next_change")));
			}
		}

		if (files.isEmpty()) {
			throw new IllegalStateException("No Oracle redo log files were found.");
		}
		return files;
	}

	private List<RedoLogFile> findArchivedRedoLogFiles(Connection connection, long startScn, long endScn) throws SQLException {
		List<RedoLogFile> files = new ArrayList<>();
		try (PreparedStatement statement = connection.prepareStatement("""
				SELECT DISTINCT name, first_change# AS first_change, next_change# AS next_change
				FROM sys.v_$archived_log
				WHERE name IS NOT NULL
				AND NVL(deleted, 'NO') = 'NO'
				AND first_change# <= ?
				AND next_change# >= ?
				ORDER BY first_change#, name
				""")) {
			statement.setLong(1, endScn);
			statement.setLong(2, startScn);
			try (ResultSet resultSet = statement.executeQuery()) {
				while (resultSet.next()) {
					files.add(new RedoLogFile(
							resultSet.getString("name"),
							RedoLogFileType.ARCHIVED,
							getNullableLong(resultSet, "first_change"),
							getNullableLong(resultSet, "next_change")));
				}
			}
		}
		return files;
	}

	@SafeVarargs
	private final List<RedoLogFile> mergeByPath(List<RedoLogFile>... candidates) {
		Map<String, RedoLogFile> files = new LinkedHashMap<>();
		for (List<RedoLogFile> candidate : candidates) {
			for (RedoLogFile file : candidate) {
				files.putIfAbsent(file.path(), file);
			}
		}
		return files.values().stream()
				.sorted(Comparator
						.comparing((RedoLogFile file) -> file.firstChange() == null ? Long.MAX_VALUE : file.firstChange())
						.thenComparing(RedoLogFile::path))
				.toList();
	}

	private List<RedoLogFile> registerRedoLogFiles(Connection connection, List<RedoLogFile> redoLogFiles) throws SQLException {
		List<RedoLogFile> registeredFiles = new ArrayList<>();
		for (int i = 0; i < redoLogFiles.size(); i++) {
			RedoLogFile file = redoLogFiles.get(i);
			String option = registeredFiles.isEmpty() ? "DBMS_LOGMNR.NEW" : "DBMS_LOGMNR.ADDFILE";
			if (registerRedoLogFile(connection, file, option)) {
				registeredFiles.add(file);
			}
		}
		if (registeredFiles.isEmpty()) {
			throw new IllegalStateException("No Oracle redo log files were registered.");
		}
		return registeredFiles;
	}

	private boolean registerRedoLogFile(Connection connection, RedoLogFile file, String option) throws SQLException {
		String sql = """
				BEGIN
				  DBMS_LOGMNR.ADD_LOGFILE(
				    LOGFILENAME => ?,
				    OPTIONS => %s
				  );
				END;
				""".formatted(option);

		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, file.path());
			statement.execute();
		}
		catch (SQLException ex) {
			log.error("[LOGMINER][REDO-LOG][ADD-FAILED] type={}, path={}, firstChange={}, nextChange={}, errorCode={}, message={}",
					file.type(),
					file.path(),
					file.firstChange(),
					file.nextChange(),
					ex.getErrorCode(),
					ex.getMessage());
			if (file.archived()) {
				log.warn("[LOGMINER][REDO-LOG][ADD-SKIPPED] archived redo log 등록 건너뜀. path={}, firstChange={}, nextChange={}, errorCode={}",
						file.path(),
						file.firstChange(),
						file.nextChange(),
						ex.getErrorCode());
				return false;
			}
			throw ex;
		}
		meterRegistry.counter(
				LogScannerMetrics.Names.REDO_LOG_REGISTERED_COUNT,
				LogScannerMetrics.Tags.MODULE, LogScannerMetrics.MODULE,
				LogScannerMetrics.Tags.TYPE, file.type().metricTag()).increment();
		return true;
	}

	private void logRegisteredFiles(List<RedoLogFile> redoLogFiles) {
		long onlineCount = redoLogFiles.stream().filter(RedoLogFile::online).count();
		long archivedCount = redoLogFiles.stream().filter(RedoLogFile::archived).count();
		log.info("[LOGMINER][REDO-LOG] registeredCount={}, onlineCount={}, archivedCount={}, firstFile={}, lastFile={}",
				redoLogFiles.size(),
				onlineCount,
				archivedCount,
				redoLogFiles.getFirst(),
				redoLogFiles.getLast());
		log.debug("[LOGMINER][REDO-LOG][FILES] files={}", redoLogFiles);
	}

	private Long getNullableLong(ResultSet resultSet, String column) throws SQLException {
		BigDecimal value = resultSet.getBigDecimal(column);
		if (value == null || value.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0) {
			return null;
		}
		return value.longValue();
	}

	// LogMiner 등록 대상 redo log 파일 정보
	public record RedoLogFile(String path, RedoLogFileType type, Long firstChange, Long nextChange) {

		public boolean online() {
			return type == RedoLogFileType.ONLINE;
		}

		public boolean archived() {
			return type == RedoLogFileType.ARCHIVED;
		}
	}

	// LogMiner 등록 파일 출처 구분
	public enum RedoLogFileType {

		ONLINE("online"),
		ARCHIVED("archived");

		private final String metricTag;

		RedoLogFileType(String metricTag) {
			this.metricTag = metricTag;
		}

		public String metricTag() {
			return metricTag;
		}
	}

}
