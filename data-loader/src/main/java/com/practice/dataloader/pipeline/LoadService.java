package com.practice.dataloader.pipeline;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.practice.dataloader.failure.FailureType;
import com.practice.dataloader.failure.LoadNonRetryableException;
import com.practice.dataloader.model.RowPayload;
import com.practice.dataloader.model.TransformEvent;
import com.practice.dataloader.observability.LoaderMetrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class LoadService {

	private final LoaderTableMappingRegistry tableMappingRegistry;
	private final List<LoadHandler> loadHandlers;
	private final MeterRegistry meterRegistry;

	// 배치 이벤트를 적재 handler별로 라우팅하고 insert는 묶음 처리
	@Transactional
	public void loadBatch(List<TransformEvent> events) {
		List<LoadTarget> targets = events.stream()
				.map(this::validate)
				.toList();

		Map<LoadRoute, List<LoadTarget>> insertTargetsByRoute = targets.stream()
				.filter(target -> "INSERT".equalsIgnoreCase(target.event().payload().operation()))
				.filter(target -> target.handler().supportsBatchInsert(target.mapping()))
				.collect(Collectors.groupingBy(target -> new LoadRoute(target.mapping(), target.handler())));

		insertTargetsByRoute.forEach((route, routeTargets) -> route.handler().loadBatchInsert(
				routeTargets.stream()
						.map(LoadTarget::event)
						.toList(),
				route.mapping()));

		targets.stream()
				.filter(target -> !"INSERT".equalsIgnoreCase(target.event().payload().operation()))
				.map(LoadTarget::event)
				.forEach(this::load);
	}

	// 단건 이벤트의 공통 검증과 handler 위임 처리
	@Transactional
	public void load(TransformEvent event) {
		Timer.Sample sample = Timer.start(meterRegistry);
		String status = LoaderMetrics.Status.SUCCESS;
		String table = tableName(event);
		String operation = operation(event);

		try {
			LoadTarget target = validate(event);
			target.handler().load(event.payload(), target.mapping());
		}
		catch (LoadNonRetryableException ex) {
			status = LoaderMetrics.Status.DLQ;
			throw ex;
		}
		catch (Exception ex) {
			status = LoaderMetrics.Status.FAILED;
			throw ex;
		}
		finally {
			recordLoadMetric(sample, table, operation, status);
		}
	}

	// 적재 전 공통 검증과 handler 매칭 수행
	public LoadTarget validate(TransformEvent event) {
		if (event == null) {
			throw nonRetryable(FailureType.UNKNOWN_LOAD_FAILED, "TransformEvent가 비어 있습니다", Map.of());
		}
		if (event.check() == null) {
			throw nonRetryable(FailureType.UNKNOWN_LOAD_FAILED, "TransformEvent check가 비어 있습니다", eventContext(event));
		}
		if (!event.check().supported()) {
			throw nonRetryable(
					FailureType.UNSUPPORTED_TABLE,
					"지원하지 않는 source table입니다",
					eventContext(event));
		}
		if (!event.check().valid()) {
			throw nonRetryable(
					FailureType.UNKNOWN_LOAD_FAILED,
					"유효하지 않은 TransformEvent입니다",
					eventContext(event));
		}
		if (event.payload() == null) {
			throw nonRetryable(FailureType.UNKNOWN_LOAD_FAILED, "TransformEvent payload가 비어 있습니다", eventContext(event));
		}

		LoadTarget target = loadTarget(event)
				.orElseThrow(() -> nonRetryable(
						FailureType.UNSUPPORTED_TABLE,
						"지원하지 않는 source table입니다",
						eventContext(event)));
		if (!target.handler().supportsOperation(event.payload().operation())) {
			throw nonRetryable(
					FailureType.UNSUPPORTED_OPERATION,
					"지원하지 않는 operation입니다",
					eventContext(event));
		}
		target.handler().validate(event.payload(), target.mapping());
		return target;
	}

	// 전달값에 맞는 테이블 매핑과 적재 처리기 선택
	private Optional<LoadTarget> loadTarget(TransformEvent event) {
		if (event == null || event.payload() == null) {
			return Optional.empty();
		}
		return tableMapping(event.payload())
				.flatMap(mapping -> loadHandler(mapping)
						.map(handler -> new LoadTarget(event, mapping, handler)));
	}

	// 전달값의 원본 테이블로 적재 매핑 조회
	private Optional<LoaderTableMapping> tableMapping(RowPayload payload) {
		if (payload == null) {
			return Optional.empty();
		}
		return tableMappingRegistry.find(payload.tableName());
	}

	// 매핑을 처리할 수 있는 첫 번째 처리기 조회
	private Optional<LoadHandler> loadHandler(LoaderTableMapping mapping) {
		return loadHandlers.stream()
				.filter(handler -> handler.supports(mapping))
				.findFirst();
	}

	// 모든 단건 적재 경로의 공통 처리 결과 메트릭 기록
	private void recordLoadMetric(Timer.Sample sample, String table, String operation, String status) {
		meterRegistry.counter(
				LoaderMetrics.Names.MYSQL_LOAD_COUNT,
				LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE,
				LoaderMetrics.Tags.TABLE, table,
				LoaderMetrics.Tags.OPERATION, operation,
				LoaderMetrics.Tags.STATUS, status)
				.increment();
		sample.stop(Timer.builder(LoaderMetrics.Names.MYSQL_LOAD_DURATION)
				.description("MySQL load duration")
				.tag(LoaderMetrics.Tags.MODULE, LoaderMetrics.MODULE)
				.tag(LoaderMetrics.Tags.TABLE, table)
				.tag(LoaderMetrics.Tags.OPERATION, operation)
				.tag(LoaderMetrics.Tags.STATUS, status)
				.register(meterRegistry));
	}

	private String tableName(TransformEvent event) {
		if (event == null || event.payload() == null || event.payload().tableName() == null) {
			return "UNKNOWN";
		}
		return event.payload().tableName();
	}

	private String operation(TransformEvent event) {
		if (event == null || event.payload() == null || event.payload().operation() == null) {
			return "UNKNOWN";
		}
		return event.payload().operation().toUpperCase(Locale.ROOT);
	}

	private Map<String, String> eventContext(TransformEvent event) {
		Map<String, String> context = new HashMap<>();
		context.put("table", tableName(event));
		context.put("operation", operation(event));
		context.put("scn", event == null || event.entry() == null ? "UNKNOWN" : String.valueOf(event.entry().scn()));
		context.put("rowNumber", event == null || event.entry() == null ? "UNKNOWN" : String.valueOf(event.entry().rowNumber()));
		if (event != null && event.check() != null && event.check().reason() != null) {
			context.put("checkReason", event.check().reason());
		}
		return context;
	}

	private LoadNonRetryableException nonRetryable(FailureType failureType, String reason, Map<String, String> context) {
		return new LoadNonRetryableException(failureType, reason, context);
	}

	public record LoadTarget(
			TransformEvent event,
			LoaderTableMapping mapping,
			LoadHandler handler) {
	}

	private record LoadRoute(
			LoaderTableMapping mapping,
			LoadHandler handler) {
	}

}
