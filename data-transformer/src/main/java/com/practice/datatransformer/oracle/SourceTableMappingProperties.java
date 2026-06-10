package com.practice.datatransformer.oracle;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

// Binds source table mappings so supported CDC tables can be expanded through configuration.
@ConfigurationProperties(prefix = "cdc.source")
public record SourceTableMappingProperties(List<SourceTableMapping> tables) {

	public SourceTableMappingProperties {
		tables = tables == null ? List.of() : List.copyOf(tables);
	}

}
