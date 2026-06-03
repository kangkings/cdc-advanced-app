package com.practice.logscanner.batch;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.ItemStream;
import org.springframework.batch.infrastructure.item.ItemStreamException;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class RedoLogPrintBatchConfig {

	@Bean
	public Job redoLogPrintJob(JobRepository jobRepository, Step redoLogPrintStep) {
		return new JobBuilder("redoLogPrintJob", jobRepository)
				.start(redoLogPrintStep)
				.build();
	}

	@Bean
	public Step redoLogPrintStep(
			JobRepository jobRepository,
			PlatformTransactionManager transactionManager,
			RedoLogItemReader redoLogItemReader,
			RedoLogItemWriter redoLogItemWriter,
			@Value("${batch.redo-log-print.chunk-size:20}") int chunkSize) {
		return new StepBuilder("redoLogPrintStep", jobRepository)
				.<RedoLogEntry, RedoLogEntry>chunk(chunkSize, transactionManager)
				.reader(redoLogItemReader)
				.writer(redoLogItemWriter)
				.stream(redoLogItemReader)
				.build();
	}

	@Bean
	@StepScope
	public RedoLogItemReader redoLogItemReader(
			DataSource dataSource,
			@Value("${batch.redo-log-print.max-rows:100}") int maxRows) {
		return new RedoLogItemReader(dataSource, maxRows);
	}

	@Bean
	@StepScope
	public RedoLogItemWriter redoLogItemWriter() {
		return new RedoLogItemWriter();
	}

	record RedoLogEntry(
			int rowNumber,
			long scn,
			Timestamp timestamp,
			String operation,
			String owner,
			String tableName,
			String username,
			String sqlRedo) {
	}

	static class RedoLogItemReader implements ItemReader<RedoLogEntry>, ItemStream {

		private final DataSource dataSource;
		private final int maxRows;

		private Connection connection;
		private PreparedStatement contentStatement;
		private ResultSet contentResultSet;
		private int rowNumber;

		RedoLogItemReader(DataSource dataSource, int maxRows) {
			this.dataSource = dataSource;
			this.maxRows = maxRows;
		}

		@Override
		public void open(ExecutionContext executionContext) throws ItemStreamException {
			try {
				connection = dataSource.getConnection();
				List<String> redoLogFiles = findRedoLogFiles(connection);
				registerRedoLogFiles(connection, redoLogFiles);
				startLogMiner(connection);
				contentStatement = prepareContentStatement(connection);
				contentResultSet = contentStatement.executeQuery();
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
			closeQuietly(connectionToClose);
			log.info("Redo log reader closed. rowCount={}, maxRows={}", rowNumber, maxRows);
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

			log.info("Redo log files found. files={}", files);
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

		private PreparedStatement prepareContentStatement(Connection connection) throws SQLException {
			PreparedStatement statement = connection.prepareStatement("""
					SELECT scn, timestamp, operation, seg_owner, table_name, username, sql_redo
					FROM v$logmnr_contents
					WHERE sql_redo IS NOT NULL
					ORDER BY scn
					FETCH FIRST ? ROWS ONLY
					""");
			statement.setInt(1, maxRows);
			return statement;
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
				log.warn("Failed to end LogMiner session.", ex);
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
				log.warn("Failed to close redo log reader resource.", ex);
			}
		}

	}

	static class RedoLogItemWriter implements ItemWriter<RedoLogEntry> {

		@Override
		public void write(Chunk<? extends RedoLogEntry> chunk) {
			log.info("Writing redo log chunk. size={}", chunk.size());
			for (RedoLogEntry entry : chunk) {
				log.info(
						"RedoLog row={} scn={} timestamp={} operation={} owner={} table={} username={} sqlRedo={}",
						entry.rowNumber(),
						entry.scn(),
						entry.timestamp(),
						entry.operation(),
						entry.owner(),
						entry.tableName(),
						entry.username(),
						entry.sqlRedo());
			}
		}

	}

}
