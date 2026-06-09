package com.practice.datagenerator.domain.user.presentation;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.practice.datagenerator.domain.user.application.UserGenerateResult;
import com.practice.datagenerator.domain.user.application.UserRateGenerateService;
import com.practice.datagenerator.domain.user.application.UserRateGenerateStatus;
import com.practice.datagenerator.domain.user.application.UserScheduledBatchService;
import com.practice.datagenerator.domain.user.application.UserUsecase;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
public class UserController {

	private final UserUsecase userUsecase;
	private final UserScheduledBatchService userScheduledBatchService;
	private final UserRateGenerateService userRateGenerateService;

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

	@PostMapping("/generate/rate/start")
	public ResponseEntity<UserRateGenerateStatus> rateStart(
			@RequestParam int rate,
			@RequestParam int durationSeconds) {
		return ResponseEntity.ok(userRateGenerateService.start(rate, durationSeconds));
	}

	@PostMapping("/generate/rate/stop")
	public ResponseEntity<UserRateGenerateStatus> rateStop() {
		return ResponseEntity.ok(userRateGenerateService.stop());
	}

	@GetMapping("/generate/rate/status")
	public ResponseEntity<UserRateGenerateStatus> rateStatus() {
		return ResponseEntity.ok(userRateGenerateService.status());
	}

}
