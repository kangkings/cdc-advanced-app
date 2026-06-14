package com.practice.logscanner;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import com.practice.logscanner.logminer.LogMinerTargetProperties;
import com.practice.logscanner.routing.RoutingProperties;

@SpringBootApplication
@EnableConfigurationProperties({LogMinerTargetProperties.class, RoutingProperties.class})
public class LogScannerApplication {

	public static void main(String[] args) {
		SpringApplication.run(LogScannerApplication.class, args);
	}

}
