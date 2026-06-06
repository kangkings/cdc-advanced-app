package com.practice.logscanner.batch.writer;

import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.stereotype.Component;

import com.practice.logscanner.batch.model.RedoLogEntry;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class RedoLogItemWriter implements ItemWriter<RedoLogEntry> {

	@Override
	public void write(Chunk<? extends RedoLogEntry> chunk) {
		for (RedoLogEntry entry : chunk) {
			log.info("[REDO-LOG-PRINT][WRITER] row={}, scn={}, timestamp={}, operation={}, owner={}, table={}, username={}, sqlRedo={}",
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
