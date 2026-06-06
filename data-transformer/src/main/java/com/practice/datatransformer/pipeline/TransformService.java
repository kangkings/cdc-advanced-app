package com.practice.datatransformer.pipeline;

import java.util.Locale;

import org.springframework.stereotype.Service;

import com.practice.datatransformer.model.CheckResult;
import com.practice.datatransformer.model.RedoEntry;
import com.practice.datatransformer.model.RowPayload;
import com.practice.datatransformer.model.TransformEvent;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class TransformService {

	private static final String MODULE = "data-transformer";
	private static final String TRANSFORM_COUNT = "data_transformer.transform.count";

	private final ValidationService validationService;
	private final MeterRegistry meterRegistry;

	public TransformEvent transform(RedoEntry entry) {
		CheckResult checkResult = validationService.check(entry);
		RowPayload payload = validationService.payload(entry, checkResult);
		if (checkResult.valid() && requiresPayloadData(entry) && (payload == null || payload.data().isEmpty())) {
			checkResult = CheckResult.invalid("payload data was not parsed from sqlRedo");
		}
		meterRegistry.counter(
				TRANSFORM_COUNT,
				"module", MODULE,
				"valid", Boolean.toString(checkResult.valid()),
				"supported", Boolean.toString(checkResult.supported()))
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
