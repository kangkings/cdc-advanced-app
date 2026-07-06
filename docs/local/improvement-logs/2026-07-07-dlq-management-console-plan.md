# 2026-07-07 DLQ Management Console Plan

CDC 파이프라인에서 DLQ는 처리 종료 지점이 아니라 운영자가 원인을 확인하고 재처리 여부를 결정하는 격리 지점으로 다룬다.
`management-console` 모듈은 DLQ 메시지를 Oracle DB에 저장하고, 조회/재처리/무시/완료 처리를 제공하는 최소 운영 모듈로 구현한다.

## Goals

- DLQ topic에 쌓인 실패 메시지를 유실 없이 Oracle DB에 저장한다.
- 운영자가 실패 유형, 원본 topic/partition/offset, payload, 실패 사유를 조회할 수 있게 한다.
- 재처리 가능한 메시지는 replay topic으로 재발행한다.
- 처리하지 않아도 되는 메시지는 무시 처리하고, 외부 보정이 끝난 메시지는 완료 처리한다.

## Scope

구현 대상:

- 신규 `management-console` 모듈 설정
- DLQ consumer
- DLQ message 저장 테이블과 JPA entity/repository
- DLQ 목록/상세 조회 API
- replay API
- ignore API
- complete API

구현 제외:

- 복잡한 자동 보정 로직
- 단계별 delay retry orchestration
- 운영 UI 화면
- DLQ payload 편집 후 재처리
- 여러 운영자 권한/승인 workflow

## Topic Policy

local 기준:

- loader DLQ topic: `cdc-loader-dlq-local`
- transformer DLQ topic: `cdc-transformer-dlq-local`
- replay topic: `cdc-loader-replay-local`

운영 기본값:

- loader DLQ topic: `cdc-loader-dlq`
- transformer DLQ topic: `cdc-transformer-dlq`
- replay topic: `cdc-loader-replay`

replay는 우선 loader 재처리를 대상으로 한다.
transformer DLQ replay가 필요하면 원본 stage에 따라 replay topic을 분리한다.

## Data Model

테이블명: `dlq_message`

필드 초안:

- `id`: surrogate key
- `stage`: 실패 stage
- `failure_type`: 실패 유형
- `reason`: 실패 사유
- `retryable`: 재시도 가능 여부
- `source_topic`: 원본 topic
- `source_partition`: 원본 partition
- `source_offset`: 원본 offset
- `original_message`: 원본 메시지 payload
- `raw_dlq_message`: DLQ 메시지 전체 payload
- `status`: 처리 상태
- `replay_topic`: 재발행 topic
- `memo`: 운영 메모
- `created_at`: DLQ 저장 시각
- `updated_at`: 상태 변경 시각
- `replayed_at`: replay 완료 시각
- `ignored_at`: 무시 처리 시각
- `completed_at`: 완료 처리 시각

중복 방지:

- `source_topic + source_partition + source_offset + stage` 기준 unique constraint를 둔다.
- 동일 DLQ 메시지를 재수신해도 중복 insert하지 않고 기존 row를 유지한다.

## Status

- `NEW`: DLQ에서 수집된 초기 상태
- `REPLAYED`: replay topic으로 재발행 완료
- `IGNORED`: 운영자가 재처리하지 않기로 결정
- `COMPLETED`: 운영자가 외부 보정 또는 수동 처리를 완료

상태 전이:

- `NEW -> REPLAYED`
- `NEW -> IGNORED`
- `NEW -> COMPLETED`

제한:

- `REPLAYED`, `IGNORED`, `COMPLETED` 상태는 중복 replay를 막는다.
- replay 발행 성공 후에만 `REPLAYED`로 변경한다.
- ignore/complete 요청에는 사유 또는 메모를 남긴다.

## Consumer Behavior

DLQ consumer는 다음 순서로 처리한다.

1. DLQ 메시지를 수신한다.
2. raw JSON에서 stage, failureType, reason, retryable, source topic/partition/offset, originalMessage를 추출한다.
3. unique key 기준으로 기존 메시지 존재 여부를 확인한다.
4. 없으면 `NEW` 상태로 저장한다.
5. 저장 성공 후 Kafka offset을 commit한다.

주의:

- DB 저장 실패 시 offset을 commit하지 않아 DLQ 메시지 유실을 막는다.
- JSON parsing 실패 시에도 raw payload를 저장할 수 있는 fallback path를 둔다.
- consumer group은 `management-console-local`처럼 loader와 분리한다.

## API

### 목록 조회

```http
GET /api/dlq/messages
```

Query:

- `status`
- `failureType`
- `stage`
- `sourceTopic`
- `from`
- `to`
- `page`
- `size`

응답:

- id
- stage
- failureType
- reason
- retryable
- sourceTopic
- sourcePartition
- sourceOffset
- status
- createdAt
- updatedAt

### 상세 조회

```http
GET /api/dlq/messages/{id}
```

응답:

- 목록 조회 필드
- originalMessage
- rawDlqMessage
- memo
- replayTopic
- replayedAt
- ignoredAt
- completedAt

### 재처리

```http
POST /api/dlq/messages/{id}/replay
```

Request:

```json
{
  "replayTopic": "cdc-loader-replay-local",
  "memo": "부모 데이터 보정 후 재처리"
}
```

처리:

1. `NEW` 상태인지 검증한다.
2. `originalMessage`를 replay topic으로 발행한다.
3. Kafka 발행 성공 후 `REPLAYED`로 상태 변경한다.
4. 이미 처리된 상태면 409 응답을 반환한다.

### 무시 처리

```http
POST /api/dlq/messages/{id}/ignore
```

Request:

```json
{
  "reason": "테스트 데이터로 판단"
}
```

처리:

1. `NEW` 상태인지 검증한다.
2. `IGNORED`로 상태 변경한다.
3. 무시 사유를 memo에 기록한다.

### 완료 처리

```http
POST /api/dlq/messages/{id}/complete
```

Request:

```json
{
  "memo": "운영자가 수동 보정 완료"
}
```

처리:

1. `NEW` 상태인지 검증한다.
2. `COMPLETED`로 상태 변경한다.
3. 완료 메모를 기록한다.

## Work Breakdown

### Step 1. Module Configuration

- `application.yml` 작성
- `application-local.yml` 작성
- Oracle datasource 설정
- Kafka consumer/producer 설정
- DLQ/replay topic 설정
- actuator/prometheus 노출 설정

### Step 2. Domain Model

- `DlqMessage` entity 작성
- `DlqMessageStatus` enum 작성
- `DlqMessageRepository` 작성
- unique constraint 적용

### Step 3. DLQ Consume

- loader/transformer DLQ topic listener 작성
- raw message parsing 로직 작성
- 중복 메시지 저장 방지
- 저장 성공 후 offset commit 정책 확인

### Step 4. Query API

- 목록 조회 API 작성
- 상세 조회 API 작성
- status/failureType/stage/sourceTopic/date filter 적용
- pagination 적용

### Step 5. Replay API

- replay request DTO 작성
- 상태 검증
- replay topic 발행
- 발행 성공 후 상태 변경
- 중복 replay 방지

### Step 6. Ignore And Complete API

- ignore API 작성
- complete API 작성
- 상태 전이 검증
- memo 기록

### Step 7. Verification

- DLQ sample message를 topic에 발행한다.
- management-console이 Oracle DB에 저장하는지 확인한다.
- 목록/상세 조회 API로 확인한다.
- replay API 호출 후 replay topic에 originalMessage가 발행되는지 확인한다.
- ignore/complete 상태 변경을 확인한다.

## Open Decisions

- replay topic을 stage별로 분리할지 여부
- DLQ raw JSON parsing 실패 시 failureType을 어떤 값으로 저장할지
- replay 후 실제 재처리 성공까지 추적할지 여부
- 운영 UI를 별도 구현할지, API와 Swagger 수준으로 둘지
