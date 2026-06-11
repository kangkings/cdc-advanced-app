package com.practice.datatransformer.pipeline;

import java.util.Locale;

import org.springframework.stereotype.Service;

import com.practice.datatransformer.model.CheckResult;
import com.practice.datatransformer.model.RedoEntry;
import com.practice.datatransformer.model.RowPayload;
import com.practice.datatransformer.model.TransformEvent;
import com.practice.datatransformer.observability.TransformerMetrics;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
// redo log 검증과 payload 변환 조합 처리
public class TransformService {

	private final ValidationService validationService;
	private final MeterRegistry meterRegistry;

	public TransformEvent transform(RedoEntry entry) {
		CheckResult checkResult = validationService.check(entry);
		RowPayload payload = validationService.payload(entry, checkResult);
		if (checkResult.valid() && requiresPayloadData(entry) && (payload == null || payload.data().isEmpty())) {
			checkResult = CheckResult.invalid("payload data was not parsed from sqlRedo");
		}
		meterRegistry.counter(
				TransformerMetrics.Names.TRANSFORM_COUNT,
				TransformerMetrics.Tags.MODULE, TransformerMetrics.MODULE,
				TransformerMetrics.Tags.VALID, Boolean.toString(checkResult.valid()),
				TransformerMetrics.Tags.SUPPORTED, Boolean.toString(checkResult.supported()))
				.increment();
		return TransformEvent.of(entry, checkResult, payload);
	}

	private boolean requiresPayloadData(RedoEntry entry) {
		if (entry == null || entry.operation() == null) {
			return false;
		}
		String operation = entry.operation().toUpperCase(Locale.ROOT);
		return "INSERT".equals(operation) || "UPDATE".equals(operation);
	}

}
