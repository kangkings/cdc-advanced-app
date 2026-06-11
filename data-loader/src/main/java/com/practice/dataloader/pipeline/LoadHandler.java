package com.practice.dataloader.pipeline;

import java.util.List;

import com.practice.dataloader.model.RowPayload;
import com.practice.dataloader.model.TransformEvent;

public interface LoadHandler {

	boolean supports(LoaderTableMapping mapping);

	boolean supportsOperation(String operation);

	boolean supportsBatchInsert(LoaderTableMapping mapping);

	void loadBatchInsert(List<TransformEvent> events, LoaderTableMapping mapping);

	void load(RowPayload payload, LoaderTableMapping mapping);

}
