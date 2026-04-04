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

| 키 | 설명 |
|----|------|
| `SLACK_BOT_TOKEN` | Slack Bot OAuth 토큰 |
| `SLACK_CHANNEL_ID` | 리포트를 전송할 채널 ID |
| `AWS_PROFILE` | AWS 프로파일명 |
| `AWS_DEFAULT_REGION` | 기본 AWS 리전 |
| `ACCOUNT_NAME` | Slack 리포트 헤더에 표시할 계정 별칭 |
| `IAM_SLACK_USER_MAP` | IAM User → Slack User ID 매핑 JSON (DM 발송용, 선택) |

---

## monitor_v2 — 실행 방법

모든 명령은 `monitor_v2/` 디렉토리에서 실행한다.

### 터미널 출력 (Slack 발송 없음)

```bash
# 비용 데이터 확인
uv-run test_cost.py

# EC2 데이터 확인
uv-run test_ec2.py
```

### Slack 전송

```bash
# 비용 리포트
uv-run test_cost_to_slack.py

# EC2 리포트
uv-run test_ec2_to_slack.py
```

#### 비용 리포트 전송 내용

- **Main**: 일일 비용 (당일/전일) + 월 누계 + 이달 예상 + Top 5 서비스
- **Thread 1**: 전체 서비스 비용 목록
- **Thread 2**: IAM User별 비용 분석 (당일 / MTD)
- **Thread 3**: 서비스 + 리전별 비용 (당일 / MTD / 예상)

#### EC2 리포트 전송 내용

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
