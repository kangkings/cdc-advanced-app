package com.practice.dataloader.observability;

// data-loader 메트릭 이름과 공통 태그 중앙 관리
public final class LoaderMetrics {

	public static final String MODULE = "data-loader";

	private LoaderMetrics() {
	}

	public static final class Names {

		public static final String KAFKA_CONSUME_COUNT = "data_loader.kafka.consume.count";
		public static final String KAFKA_LISTENER_DURATION = "data_loader.kafka.listener.duration";
		public static final String MYSQL_LOAD_DURATION = "data_loader.mysql.load.duration";
		public static final String MYSQL_LOAD_COUNT = "data_loader.mysql.load.count";
		public static final String MYSQL_INSERT_DURATION = "data_loader.mysql.insert.duration";
		public static final String MYSQL_INSERT_COUNT = "data_loader.mysql.insert.count";
		public static final String MYSQL_BATCH_INSERT_DURATION = "data_loader.mysql.batch.insert.duration";
		public static final String MYSQL_BATCH_INSERT_COUNT = "data_loader.mysql.batch.insert.count";
		public static final String MYSQL_UPDATE_DURATION = "data_loader.mysql.update.duration";
		public static final String MYSQL_DELETE_DURATION = "data_loader.mysql.delete.duration";
		public static final String FAILURE_COUNT = "data_loader.failure.count";
		public static final String DLQ_PUBLISH_COUNT = "data_loader.dlq.publish.count";

		private Names() {
		}
	}

	public static final class Tags {

		public static final String MODULE = "module";
		public static final String STATUS = "status";
		public static final String TABLE = "table";
		public static final String OPERATION = "operation";
		public static final String TOPIC = "topic";
		public static final String FAILURE_TYPE = "failureType";
		public static final String RETRYABLE = "retryable";

		private Tags() {
		}
	}

	public static final class Status {

		public static final String SUCCESS = "SUCCESS";
		public static final String FAILED = "FAILED";
		public static final String SKIPPED = "SKIPPED";
		public static final String DLQ = "DLQ";

		private Status() {
		}
	}

}
