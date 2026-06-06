package com.practice.logscanner.logminer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class LogMinerConnectionFactory {

	private final String driverClassName;
	private final String url;
	private final String username;
	private final String password;

	public LogMinerConnectionFactory(
			@Value("${logminer.datasource.driver-class-name:oracle.jdbc.OracleDriver}") String driverClassName,
			@Value("${logminer.datasource.url}") String url,
			@Value("${logminer.datasource.username}") String username,
			@Value("${logminer.datasource.password}") String password) {
		this.driverClassName = driverClassName;
		this.url = url;
		this.username = username;
		this.password = password;
	}

	public Connection getConnection() throws SQLException {
		try {
			Class.forName(driverClassName);
		}
		catch (ClassNotFoundException ex) {
			throw new IllegalStateException("LogMiner JDBC driver was not found.", ex);
		}
		return DriverManager.getConnection(url, username, password);
	}

}
