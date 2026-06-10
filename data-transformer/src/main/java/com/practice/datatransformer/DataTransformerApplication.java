package com.practice.datatransformer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class DataTransformerApplication {

	public static void main(String[] args) {
		SpringApplication.run(DataTransformerApplication.class, args);
	}

}
