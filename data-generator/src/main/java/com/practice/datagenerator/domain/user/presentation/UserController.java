package com.practice.datagenerator.domain.user.presentation;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.practice.datagenerator.domain.user.application.UserGenerateResult;
import com.practice.datagenerator.domain.user.application.UserScheduledBatchService;
import com.practice.datagenerator.domain.user.application.UserUsecase;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
public class UserController {

	private final UserUsecase userUsecase;
	private final UserScheduledBatchService userScheduledBatchService;

	@PostMapping("/generate")
	public ResponseEntity<UserGenerateResult> generate(
			@RequestParam int count) {
		return ResponseEntity.ok(userUsecase.generateUsers(count));
	}

	@PostMapping("/generate/schedule/start")
	public ResponseEntity<Map<String, Object>> scheduleStart(
			@RequestParam int count) {
		userScheduledBatchService.start(count);
		return ResponseEntity.ok(Map.of(
				"status", "started",
				"count", count));
	}

	@PostMapping("/generate/schedule/stop")
	public ResponseEntity<Map<String, Object>> scheduleStop() {
		userScheduledBatchService.stop();
		return ResponseEntity.ok(Map.of(
				"status", "stopped",
				"running", userScheduledBatchService.isRunning()));
	}

}
