# cloud-usage

AWS 클라우드 비용 및 리소스 사용 현황을 모니터링하고 Slack으로 리포트를 전송하는 프로젝트.

---

## 환경 설정

### 1. 의존성 설치

```bash
uv sync
```

### 2. 환경변수

프로젝트 루트에 `.env` 파일을 생성한다.

#### 공통

| 키 | 설명 |
|----|------|
| `SLACK_BOT_TOKEN` | Slack Bot OAuth 토큰 |
| `SLACK_CHANNEL_ID` | 리포트를 전송할 채널 ID |
| `AWS_PROFILE` | AWS 프로파일명 |
| `AWS_DEFAULT_REGION` | 기본 AWS 리전 |
| `ACCOUNT_NAME` | Slack 리포트 헤더에 표시할 계정 별칭 |

#### CUR (Athena) 전용 추가 변수

| 키 | 필수 | 설명 |
|----|------|------|
| `ATHENA_OUTPUT_LOCATION` | ✅ | Athena 쿼리 결과 저장 S3 URI |
| `ATHENA_DATABASE` | ⬜ | Athena 데이터베이스명 |
| `ATHENA_WORKGROUP` | ⬜ | Athena 워크그룹 |

> `ATHENA_OUTPUT_LOCATION`은 CUR 데이터 위치가 아닌 Athena **쿼리 결과**가 저장되는 S3 경로다.
> AWS 콘솔 → Athena → Settings → Query result location 값과 동일.

#### AI 분석 (Main 3) 전용 추가 변수

| 키 | 필수 | 설명 |
|----|------|------|
| `BEDROCK_MODEL_ID` | ✅ | Bedrock 모델 ID (예: `amazon.nova-micro-v1:0`) |
| `BEDROCK_REGION` | ✅ | Bedrock 리전 (예: `us-east-1`) |
| `NEW_COST_THRESHOLD` | ⬜ | 이번 달 신규 발생 판단 임계값 (기본: `10`, 단위: USD) |

#### IAM → Slack 사용자 매핑 (DM 발송용, 선택)

`monitor_v2/iam_to_slack.json` 파일로 관리한다.

```json
{
  "alice": "U012ABC3456",
  "bob":   "U098XYZ7890"
}
```

파일이 없을 경우 환경변수 `IAM_SLACK_USER_MAP` (JSON 문자열)을 폴백으로 사용한다.

---

## monitor_v2 — 실행 방법

### 데이터 소스 비교

| 항목 | Cost Explorer (CE) | Athena CUR |
|------|-------------------|------------|
| 데이터 소스 | AWS Cost Explorer API | Athena `hyu_ddps_logs.cur_logs` |
| 추가 환경변수 | 없음 | `ATHENA_OUTPUT_LOCATION` 필요 |
| 쿼리 레퍼런스 | — | `monitor_v2/cost/queries.sql` |
| 메시지 구성 | 동일 | 동일 |
| forecast | CE forecast API | CE forecast API (공통) |

---

### Cost Explorer (CE) 기반

#### 터미널 출력 (Slack 발송 없음)

```bash
uv run python -m monitor_v2.test_cost
uv run python -m monitor_v2.test_ec2
```

#### Slack 전송

```bash
# 비용 리포트
uv run python -m monitor_v2.test_cost_to_slack

# EC2 리포트
uv run python -m monitor_v2.test_ec2_to_slack
```

---

### Athena CUR 기반

#### Slack 전송

```bash
# 비용 리포트
uv run python -m monitor_v2.test_cost_cur_to_slack

# EC2 리포트
uv run python -m monitor_v2.test_ec2_cur_to_slack
```

---

### AI 분석 리포트 (Main 3)

CUR 데이터를 Bedrock Nova Micro로 분석해 자연어 한국어 요약을 Slack에 전송한다.

```bash
# AI 분석 리포트 (Main 3)
uv run python -m monitor_v2.test_main3
```

리포트 구성:

- **상단 수치**: 어제 총비용 / 이번 달 누계 (N일 경과) / 월말 예상
- **AI 요약 (3문단 통찰형)**:
  1. 결론 한 줄 + 월간 흐름
  2. 어제 비용의 driver 분석 (서비스 비중 + IAM × 인스턴스 타입 + 1대 평균 가동 시간 + 서비스별 페이스 비교)
  3. 이번 달 들어 새로 비용이 발생한 서비스 / 동일 사용자 신호 (있을 때만, 없으면 생략)
- **Q9/Q10/Q11 raw 표**: 서비스별 / 타입별 / 리소스 ID별 어제 vs 그제 변화 — drill-down 검증용

---

### 리포트 전송 내용 (CE·CUR 공통)

#### 비용 리포트

- **Main**: 일일 비용 (당일/전일) + 월 누계 + 이달 예상 + Top 5 서비스
- **Thread 1**: 전체 서비스 비용 목록
- **Thread 2**: IAM User별 비용 분석 (당일 / MTD)
- **Thread 3**: 서비스 + 리전별 비용 (당일 / MTD / 예상)

#### EC2 리포트

- **Main**: EC2 비용 (당일/전일/MTD) + 활성 리전 + Top 5 인스턴스 타입 + Top 5 IAM User
- **Thread 1**: 리전별 인스턴스 상세 (On-Demand/Spot × running/stopped/terminated)
- **Thread 2**: 미사용 EBS 볼륨 + Snapshot 목록
- **Thread 3**: IAM User별 EC2 비용 (당일 / MTD / 이달 예상)

---

## print_test — 실행 방법

AWS API 응답 구조 확인용 탐색 스크립트. 모든 명령은 프로젝트 루트에서 실행한다.

```bash
# EC2
uv run python -m print_test.ec2.describe_instances
uv run python -m print_test.ec2.describe_volumes

# Cost Explorer
uv run python -m print_test.cost_explorer.get_cost_and_usage

# CloudTrail
uv run python -m print_test.cloudtrail.lookup_events

# Lambda
uv run python -m print_test.lambda_fn.list_functions
```
