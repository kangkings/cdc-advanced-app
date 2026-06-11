package com.practice.dataloader.pipeline;

import java.util.List;

import com.practice.dataloader.model.RowPayload;
import com.practice.dataloader.model.TransformEvent;

// table별 적재 전략을 분리하는 handler 계약
public interface LoadHandler {

	boolean supports(LoaderTableMapping mapping);

	boolean supportsOperation(String operation);

	boolean supportsBatchInsert(LoaderTableMapping mapping);

	default void validate(RowPayload payload, LoaderTableMapping mapping) {
	}

	void loadBatchInsert(List<TransformEvent> events, LoaderTableMapping mapping);

	void load(RowPayload payload, LoaderTableMapping mapping);

}
