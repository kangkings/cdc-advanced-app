package com.practice.datagenerator.observability;

// data-generator 메트릭 이름과 공통 태그 중앙 관리
public final class GeneratorMetrics {

	public static final String MODULE = "data-generator";

	private GeneratorMetrics() {
	}

	public static final class Names {

		public static final String USER_READER_DURATION = "data_generator.user.reader.duration";
		public static final String USER_READER_COUNT = "data_generator.user.reader.count";
		public static final String USER_WRITER_DURATION = "data_generator.user.writer.duration";
		public static final String USER_WRITER_CHUNK_COUNT = "data_generator.user.writer.chunk.count";
		public static final String USER_WRITER_ITEM_COUNT = "data_generator.user.writer.item.count";
		public static final String USER_JOB_DURATION = "data_generator.user.job.duration";
		public static final String USER_JOB_FAILURE_COUNT = "data_generator.user.job.failure.count";
		public static final String USER_STEP_DURATION = "data_generator.user.step.duration";
		public static final String USER_READ_COUNT = "data_generator.user.read.count";
		public static final String USER_WRITE_COUNT = "data_generator.user.write.count";
		public static final String USER_FILTER_COUNT = "data_generator.user.filter.count";
		public static final String USER_SKIP_COUNT = "data_generator.user.skip.count";
		public static final String USER_PROCESSOR_DURATION = "data_generator.user.processor.duration";
		public static final String USER_PROCESS_COUNT = "data_generator.user.process.count";
		public static final String USER_PROCESS_ERROR_COUNT = "data_generator.user.process.error.count";
		public static final String RATE_TICK_COUNT = "data_generator.rate.tick.count";
		public static final String RATE_INSERT_COUNT = "data_generator.rate.insert.count";
		public static final String RATE_FAILURE_COUNT = "data_generator.rate.failure.count";
		public static final String RATE_INSERT_DURATION = "data_generator.rate.insert.duration";

		private Names() {
		}
	}

	public static final class Tags {

		public static final String MODULE = "module";
		public static final String JOB = "job";
		public static final String STEP = "step";
		public static final String STATUS = "status";

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
