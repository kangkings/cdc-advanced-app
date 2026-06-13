package com.practice.logscanner.logminer;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

// LogMiner 조회 대상 owner.table 설정 바인딩
@ConfigurationProperties(prefix = "logminer")
public record LogMinerTargetProperties(
		List<String> targets,
		boolean committedDataOnly,
		long maxScanScnRange,
		TransactionDiagnostics transactionDiagnostics) {

	public LogMinerTargetProperties {
		if (transactionDiagnostics == null) {
			transactionDiagnostics = new TransactionDiagnostics(false, 20, false);
		}
	}

	// LogMiner transaction 컬럼 관측용 설정
	public record TransactionDiagnostics(boolean enabled, int maxLogs, boolean publishEnabled) {
	}
}
