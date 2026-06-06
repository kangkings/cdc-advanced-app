package com.practice.logscanner;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import com.practice.logscanner.logminer.LogMinerTargetProperties;

@SpringBootApplication
@EnableConfigurationProperties(LogMinerTargetProperties.class)
public class LogScannerApplication {

	public static void main(String[] args) {
		SpringApplication.run(LogScannerApplication.class, args);
	}

}
