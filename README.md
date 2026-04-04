# cloud-usage

A repository to store source codes to monitor cloud cost and track issues related to cloud usages.

---

## 목차

- [환경 설정](#환경-설정)
- [monitor\_v2 — 실행 방법](#monitor_v2--실행-방법)
- [print\_test — 실행 방법](#print_test--실행-방법)

---

## 환경 설정

### 1. 의존성 동기화 (uv)

```bash
# 프로젝트 루트에서 실행
uv sync
```

`pyproject.toml`에 정의된 모든 의존성(`boto3`, `slack_sdk`, `google-cloud-bigquery` 등)이 가상환경에 설치된다.

> uv가 없는 경우: `pip install uv` 또는 [uv 공식 설치 가이드](https://docs.astral.sh/uv/getting-started/installation/) 참고

---

### 2. `.env` 파일 구성

프로젝트 루트(`cloud-usage/`)에 `.env` 파일을 생성한다.

```env
# Slack
SLACK_BOT_TOKEN="xoxb-..."
SLACK_CHANNEL="cloud-usage-test"

# AWS (프로파일 기반 인증 사용 시)
AWS_PROFILE="default"

# AWS 계정 별칭 (Slack 리포트 헤더에 표시)
ACCOUNT_NAME="hyu-ddps"

# IAM User → Slack User ID 매핑 (DM 발송용, 선택)
# IAM_SLACK_USER_MAP='{"IAMUser:123:alice": "U012ABC3456"}'
```

> `.env` 파일은 `.gitignore`에 포함되어 있으므로 커밋되지 않는다.
> 테스트용 Slack 채널은 `cloud-usage-test`를 사용한다.

`setup_environment()`가 실행되면 `.env`를 읽어 `os.environ`에 자동으로 주입한다.

---

## monitor\_v2 — 실행 방법

모든 명령은 **프로젝트 루트(`cloud-usage/`)** 에서 실행한다.

### 터미널 Print 전용 (Slack 발송 없음)

수집한 데이터를 터미널에 출력만 하는 단독 테스트 스크립트.

#### 비용 데이터 확인 (`cost/data.py`)

```bash
uv run python -m monitor_v2.test_cost
# 또는
python -m monitor_v2.test_cost
```

출력 내용:
- D-1 / D-2 / 전월 동일일 서비스별 비용
- MTD 누계 + 잔여 예측
- IAM User(aws:createdBy)별 비용
- 서비스 + 리전별 비용

#### EC2 데이터 확인 (`ec2/data.py`)

```bash
uv run python -m monitor_v2.test_ec2
# 또는
python -m monitor_v2.test_ec2
```

출력 내용:
- 전 리전 running / stopped / terminated 인스턴스 목록
- 인스턴스 타입별 D-1 비용
- 미사용 EBS 볼륨 목록
- 미사용 Snapshot 목록

---

### Slack 채널로 전송 (`cloud-usage-test`)

`.env`에 `SLACK_CHANNEL=cloud-usage-test`가 설정되어 있어야 한다.

#### 비용 리포트 → Slack 전송 (Main 1)

```bash
uv run python -m monitor_v2.test_cost_to_slack
# 또는
python -m monitor_v2.test_cost_to_slack
```

전송 내용:
- Main 1 메시지: 전체 서비스 비용 (당일 / 전일 / MTD / 예상)
- Thread 2: IAM User별 비용 분석
- Thread 3: 서비스 + 리전별 비용

#### EC2 리포트 → Slack 전송 (Main 2 + Thread 1~3)

```bash
uv run python -m monitor_v2.test_ec2_to_slack
# 또는
python -m monitor_v2.test_ec2_to_slack
```

전송 내용:
- Main 2 메시지: EC2 비용 요약 + 활성 리전 + Top 5 인스턴스 타입 + Top 5 IAM User
- Thread 1: 리전별 인스턴스 상세 (리전 >> On-Demand/Spot >> running/stopped/terminated 순 마크다운 테이블)
- Thread 2: 미사용 EBS 볼륨 + Snapshot 목록
- Thread 3: IAM User별 EC2 비용 (당일 / MTD / 이달 예상)

---

## print\_test — 실행 방법

개별 AWS API 응답 구조를 확인하기 위한 탐색 스크립트. 각 스크립트는 독립적으로 실행 가능하며 터미널에 출력만 한다.

모든 명령은 **프로젝트 루트(`cloud-usage/`)** 에서 실행한다.

### EC2

```bash
# EC2 인스턴스 조회 (describe_instances)
uv run python -m print_test.ec2.describe_instances

# 미사용 EBS 볼륨 조회 (describe_volumes)
uv run python -m print_test.ec2.describe_volumes
```

### Cost Explorer

```bash
# 서비스별 / 인스턴스 타입별 비용 조회 (get_cost_and_usage)
uv run python -m print_test.cost_explorer.get_cost_and_usage

# IAM User별 비용 집계 (aws:createdBy 태그 기반)
uv run python -m print_test.cost_explorer.aws_createdBy
```

> `aws_createdBy` 실행 전에 AWS Billing 콘솔 > Cost Allocation Tags에서 `aws:createdBy` 태그가 활성화되어 있어야 한다. 활성화 후 최대 24시간이 지나야 Cost Explorer에서 조회된다.

### CloudTrail

```bash
# EC2 인스턴스 상태 변화 이벤트 조회 (lookup_events)
uv run python -m print_test.cloudtrail.lookup_events
```

### Lambda

```bash
# Lambda 함수 목록 조회 (list_functions)
uv run python -m print_test.lambda_fn.list_functions
```
