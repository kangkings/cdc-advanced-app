package com.practice.logscanner.observability;

// log-scanner 메트릭 이름과 공통 태그 중앙 관리
public final class LogScannerMetrics {

	public static final String MODULE = "log-scanner";

	private LogScannerMetrics() {
	}

	public static final class Names {

		public static final String REDO_LOG_READER_DURATION = "log_scanner.redo_log.reader.duration";
		public static final String REDO_LOG_READER_COUNT = "log_scanner.redo_log.reader.count";
		public static final String KAFKA_PUBLISH_CHUNK_DURATION = "log_scanner.kafka.publish.chunk.duration";
		public static final String KAFKA_PUBLISH_CHUNK_COUNT = "log_scanner.kafka.publish.chunk.count";
		public static final String KAFKA_PUBLISH_ENTRY_COUNT = "log_scanner.kafka.publish.entry.count";
		public static final String KAFKA_PUBLISH_SUCCESS_COUNT = "log_scanner.kafka.publish.success.count";
		public static final String KAFKA_PUBLISH_FAILURE_COUNT = "log_scanner.kafka.publish.failure.count";
		public static final String REDO_LOG_JOB_DURATION = "log_scanner.redo_log.job.duration";
		public static final String REDO_LOG_JOB_FAILURE_COUNT = "log_scanner.redo_log.job.failure.count";
		public static final String REDO_LOG_STEP_DURATION = "log_scanner.redo_log.step.duration";
		public static final String REDO_LOG_READ_COUNT = "log_scanner.redo_log.read.count";
		public static final String REDO_LOG_WRITE_COUNT = "log_scanner.redo_log.write.count";
		public static final String REDO_LOG_SKIP_COUNT = "log_scanner.redo_log.skip.count";
		public static final String REDO_LOG_WRITER_DURATION = "log_scanner.redo_log.writer.duration";
		public static final String REDO_LOG_WRITER_CHUNK_COUNT = "log_scanner.redo_log.writer.chunk.count";
		public static final String REDO_LOG_WRITER_ITEM_COUNT = "log_scanner.redo_log.writer.item.count";
		public static final String REDO_LOG_WRITER_ERROR_COUNT = "log_scanner.redo_log.writer.error.count";
		public static final String REDO_LOG_REGISTERED_COUNT = "log_scanner.redo_log.registered.count";
		public static final String REDO_LOG_SCAN_WINDOW_COUNT = "log_scanner.redo_log.scan.window.count";
		public static final String REDO_LOG_SCAN_WINDOW_SCN_RANGE = "log_scanner.redo_log.scan.window.scn.range";
		public static final String REDO_LOG_SCAN_ENTRY_COUNT = "log_scanner.redo_log.scan.entry.count";
		public static final String CHECKPOINT_SAVE_COUNT = "log_scanner.checkpoint.save.count";
		public static final String LOGMINER_START_FAILURE_COUNT = "log_scanner.logminer.start.failure.count";
		public static final String PENDING_TRANSACTION_EVENT_COUNT = "log_scanner.pending_transaction.event.count";
		public static final String PENDING_TRANSACTION_SAVE_DURATION = "log_scanner.pending_transaction.save.duration";
		public static final String PENDING_TRANSACTION_COMMIT_DURATION = "log_scanner.pending_transaction.commit.duration";
		public static final String PENDING_TRANSACTION_ROLLBACK_DURATION = "log_scanner.pending_transaction.rollback.duration";
		public static final String PENDING_TRANSACTION_FIND_DURATION = "log_scanner.pending_transaction.find.duration";
		public static final String PENDING_TRANSACTION_DELETE_DURATION = "log_scanner.pending_transaction.delete.duration";
		public static final String PENDING_TRANSACTION_CONTROL_SKIP_COUNT = "log_scanner.pending_transaction.control.skip.count";

		private Names() {
		}
	}

	public static final class Tags {

		public static final String MODULE = "module";
		public static final String TOPIC = "topic";
		public static final String STATUS = "status";
		public static final String JOB = "job";
		public static final String STEP = "step";
		public static final String TYPE = "type";
		public static final String ERROR = "error";

		private Tags() {
		}
	}

	public static final class Status {

		public static final String SUCCESS = "SUCCESS";
		public static final String FAILED = "FAILED";

		private Status() {
		}
	}

}
