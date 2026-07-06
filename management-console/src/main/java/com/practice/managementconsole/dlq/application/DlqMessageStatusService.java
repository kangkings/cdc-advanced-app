package com.practice.managementconsole.dlq.application;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import com.practice.managementconsole.dlq.domain.DlqMessage;
import com.practice.managementconsole.dlq.domain.DlqMessageRepository;

import lombok.RequiredArgsConstructor;

import static org.springframework.http.HttpStatus.CONFLICT;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@Service
@RequiredArgsConstructor
// DLQ 메시지 운영 상태 변경 처리
public class DlqMessageStatusService {

	private final DlqMessageRepository dlqMessageRepository;

	// DLQ 메시지 무시 처리
	@Transactional
	public DlqMessage ignore(Long id, String reason) {
		DlqMessage message = findMessage(id);
		try {
			message.ignore(reason);
			return message;
		}
		catch (IllegalStateException ex) {
			throw new ResponseStatusException(CONFLICT, ex.getMessage(), ex);
		}
	}

	// DLQ 메시지 완료 처리
	@Transactional
	public DlqMessage complete(Long id, String memo) {
		DlqMessage message = findMessage(id);
		try {
			message.complete(memo);
			return message;
		}
		catch (IllegalStateException ex) {
			throw new ResponseStatusException(CONFLICT, ex.getMessage(), ex);
		}
	}

	private DlqMessage findMessage(Long id) {
		return dlqMessageRepository.findById(id)
				.orElseThrow(() -> new ResponseStatusException(NOT_FOUND, "DLQ 메시지를 찾을 수 없습니다. id=" + id));
	}

}
