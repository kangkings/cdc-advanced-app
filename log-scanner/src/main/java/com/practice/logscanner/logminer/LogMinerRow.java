package com.practice.logscanner.logminer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.practice.logscanner.batch.model.RedoLogEntry;

// LogMiner 원본 row를 DML과 transaction control 처리 전에 담는 내부 모델
public record LogMinerRow(
		int rowNumber,
		long scn,
		Timestamp timestamp,
		String operation,
		Integer operationCode,
		String owner,
		String tableName,
		String username,
		String rowId,
		String sqlRedo,
		String xid,
		Long xidusn,
		Long xidslt,
		Long xidsqn,
		String rsId,
		Long ssn) {

	public static LogMinerRow from(ResultSet resultSet, int rowNumber) throws SQLException {
		return new LogMinerRow(
				rowNumber,
				resultSet.getLong("scn"),
				resultSet.getTimestamp("timestamp"),
				resultSet.getString("operation"),
				nullableInteger(resultSet, "operation_code"),
				resultSet.getString("seg_owner"),
				resultSet.getString("table_name"),
				resultSet.getString("username"),
				resultSet.getString("row_id"),
				resultSet.getString("sql_redo"),
				resultSet.getString("xid"),
				nullableLong(resultSet, "xidusn"),
				nullableLong(resultSet, "xidslt"),
				nullableLong(resultSet, "xidsqn"),
				resultSet.getString("rs_id"),
				nullableLong(resultSet, "ssn"));
	}

	public boolean dml() {
		return "INSERT".equals(operation) || "UPDATE".equals(operation) || "DELETE".equals(operation);
	}

	public boolean commit() {
		return "COMMIT".equals(operation);
	}

	public boolean rollback() {
		return "ROLLBACK".equals(operation);
	}

	public RedoLogEntry toEntry() {
		return new RedoLogEntry(
				rowNumber,
				scn,
				timestamp,
				operation,
				operationCode,
				owner,
				tableName,
				username,
				rowId,
				sqlRedo,
				xid,
				xidusn,
				xidslt,
				xidsqn,
				rsId,
				ssn);
	}

	private static Long nullableLong(ResultSet resultSet, String column) throws SQLException {
		long value = resultSet.getLong(column);
		if (resultSet.wasNull()) {
			return null;
		}
		return value;
	}

	private static Integer nullableInteger(ResultSet resultSet, String column) throws SQLException {
		int value = resultSet.getInt(column);
		if (resultSet.wasNull()) {
			return null;
		}
		return value;
	}

}
