package com.practice.logscanner.logminer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class RedoLogFileRegistrar {

	public List<String> registerAll(Connection connection) throws SQLException {
		List<String> redoLogFiles = findRedoLogFiles(connection);
		registerRedoLogFiles(connection, redoLogFiles);
		log.info("[LOGMINER][REDO-LOG] registeredCount={}, files={}", redoLogFiles.size(), redoLogFiles);
		return redoLogFiles;
	}

	private List<String> findRedoLogFiles(Connection connection) throws SQLException {
		List<String> files = new ArrayList<>();
		try (Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery("""
						SELECT DISTINCT member
						FROM sys.v_$logfile
						ORDER BY member
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

}
