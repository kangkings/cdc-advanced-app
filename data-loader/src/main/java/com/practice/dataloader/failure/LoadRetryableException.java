package com.practice.dataloader.failure;

public class LoadRetryableException extends RuntimeException {

	public LoadRetryableException(String message, Throwable cause) {
		super(message, cause);
	}

}
