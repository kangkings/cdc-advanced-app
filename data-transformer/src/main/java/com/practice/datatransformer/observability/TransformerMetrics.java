package com.practice.datatransformer.observability;

// data-transformer 메트릭 이름과 공통 태그 중앙 관리
public final class TransformerMetrics {

	public static final String MODULE = "data-transformer";

	private TransformerMetrics() {
	}

	public static final class Names {

		public static final String KAFKA_CONSUME_COUNT = "data_transformer.kafka.consume.count";
		public static final String PIPELINE_PROCESS_DURATION = "data_transformer.pipeline.process.duration";
		public static final String TRANSFORM_COUNT = "data_transformer.transform.count";
		public static final String ORACLE_ROW_LOOKUP_DURATION = "data_transformer.oracle.row_lookup.duration";
		public static final String ORACLE_ROW_LOOKUP_COUNT = "data_transformer.oracle.row_lookup.count";
		public static final String KAFKA_PUBLISH_COUNT = "data_transformer.kafka.publish.count";
		public static final String FAILURE_COUNT = "data_transformer.failure.count";
		public static final String DLQ_PUBLISH_COUNT = "data_transformer.dlq.publish.count";

		private Names() {
		}
	}

	public static final class Tags {

		public static final String MODULE = "module";
		public static final String STATUS = "status";
		public static final String TABLE = "table";
		public static final String OPERATION = "operation";
		public static final String TOPIC = "topic";
		public static final String VALID = "valid";
		public static final String SUPPORTED = "supported";
		public static final String FAILURE_TYPE = "failureType";
		public static final String RETRYABLE = "retryable";

		private Tags() {
		}
	}

	public static final class Status {

		public static final String SUCCESS = "SUCCESS";
		public static final String FAILED = "FAILED";
		public static final String DLQ = "DLQ";

		private Status() {
		}
	}

}
