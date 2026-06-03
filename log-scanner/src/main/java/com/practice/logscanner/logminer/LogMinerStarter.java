package com.practice.logscanner.logminer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LogMinerStarter implements ApplicationRunner, DisposableBean {

	private final boolean enabled;
	private final DataSource dataSource;

	private Connection connection;
	private boolean started;

	public LogMinerStarter(
			@Value("${logminer.starter.enabled:false}") boolean enabled,
			DataSource dataSource) {
		this.enabled = enabled;
		this.dataSource = dataSource;
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		if (!enabled) {
			log.info("LogMiner startup is disabled.");
			return;
		}

		connection = dataSource.getConnection();

		List<String> redoLogFiles = findRedoLogFiles(connection);
		registerRedoLogFiles(connection, redoLogFiles);
		startLogMiner(connection);
		started = true;

		log.info("LogMiner started. registeredRedoLogFiles={}", redoLogFiles);
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

	private void startLogMiner(Connection connection) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			statement.execute("""
					BEGIN
					  DBMS_LOGMNR.START_LOGMNR(
					    OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG
					             + DBMS_LOGMNR.COMMITTED_DATA_ONLY
					             + DBMS_LOGMNR.PRINT_PRETTY_SQL
					  );
					END;
					""");
		}
	}

	@Override
	public void destroy() {
		if (connection == null) {
			return;
		}

		try {
			if (started) {
				endLogMiner(connection);
				log.info("LogMiner ended.");
			}
		}
		catch (SQLException ex) {
			log.warn("Failed to end LogMiner session.", ex);
		}
		finally {
			closeConnection();
		}
	}

	private void endLogMiner(Connection connection) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			statement.execute("""
					BEGIN
					  DBMS_LOGMNR.END_LOGMNR;
					END;
					""");
		}
	}

	private void closeConnection() {
		try {
			connection.close();
		}
		catch (SQLException ex) {
			log.warn("Failed to close LogMiner connection.", ex);
		}
	}

}
