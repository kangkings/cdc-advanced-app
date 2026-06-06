package com.practice.logscanner.logminer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

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
	private final LogMinerConnectionFactory logMinerConnectionFactory;
	private final RedoLogFileRegistrar redoLogFileRegistrar;

	private Connection connection;
	private boolean started;

	public LogMinerStarter(
			@Value("${logminer.starter.enabled:false}") boolean enabled,
			LogMinerConnectionFactory logMinerConnectionFactory,
			RedoLogFileRegistrar redoLogFileRegistrar) {
		this.enabled = enabled;
		this.logMinerConnectionFactory = logMinerConnectionFactory;
		this.redoLogFileRegistrar = redoLogFileRegistrar;
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		if (!enabled) {
			log.info("LogMiner startup is disabled.");
			return;
		}

		connection = logMinerConnectionFactory.getConnection();

		List<String> redoLogFiles = redoLogFileRegistrar.registerAll(connection);
		startLogMiner(connection);
		started = true;

		log.info("LogMiner started. registeredRedoLogFiles={}", redoLogFiles);
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
