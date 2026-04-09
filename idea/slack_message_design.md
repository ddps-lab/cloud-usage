# Slack 비용 모니터링 메시지 설계

> 목적: 연구원들이 단일 AWS 계정 내 다중 region에서 사용하는 리소스 비용을 매일 1회 Slack으로 공유.
> 기준: 실서비스(CloudZero, nOps, Finout, AWS Cost Explorer 등) 조사 인사이트 반영.
>
> **메시지 구조 개요**: 채널에 2개의 독립된 Main 메시지가 순차 발송됨.
> - **Main 1**: 전체 비용 요약 + Top 5 서비스 (비용 개요)
> - **Main 2**: EC2 전용 상세 리포트 (인스턴스 단위 분석)

---

## 설계 원칙

| 원칙 | 이유 |
|------|------|
| **Main은 간결** | 채널이 지저분해지면 아무도 안 읽음 (CloudZero 사례) |
| **Thread는 상세** | 궁금한 사람만 펼쳐보는 구조 (nOps 사례) |
| **비교가 핵심** | 절대 금액보다 "오늘이 어제보다 얼마나 달라졌는가"가 핵심 행동 유도 |
| **Top 5 중심** | 80% 이상의 비용은 5개 서비스에서 발생 (파레토 원칙) |
| **이상만 강조** | 정상 범위면 안 읽어도 됨, 이상 시에만 눈에 띄어야 함 |
| **단일 계정 기준** | 다중 계정 비교 불필요, 구조 단순화로 가독성 향상 |
| **책임 소재 명시** | IAM User별 분류로 누가 무엇을 얼마나 쓰는지 투명하게 공유 |
| **낭비 리소스 개인 알림** | 채널 공지보다 당사자 DM이 행동 유도에 효과적 |

---

## Main 1 메시지 설계 — 전체 비용 요약

> 채널에 노출되는 첫 번째 메시지. 30초 안에 오늘 전체 비용 상황을 파악할 수 있어야 함.

### 구성 요소

- **헤더**: 보고 대상 날짜 (어제 날짜 기준) + 계정명 (Account alias)
- **일일 총비용**: 전일 대비 금액 차이 + 퍼센트 변화 + 방향 아이콘 (📈/📉/➡️)
- **월단위 누적**: 이번 달 누계 비용 + 전월 동기 대비 변화율
- **Top 5 서비스**: 비용 내림차순, 각 서비스별 전일 대비 변화율 + 전월 동기 대비 변화율
- **이상 마커**: 전일 대비 20% 이상 증가 서비스에 ⚠️ 표시
- **Thread 안내**: "자세한 내용은 스레드에서 확인" 한 줄

### Main 1 메시지 예시

```
📊 AWS Cost Report — 2026-03-25
계정: hyu-ddps

일일 비용: $123.45   (어제 $110.00 / +$13.45 / +12.2% 📈)
월 누계:   $892.00   (전월 동기 $850.00 / +$42.00 / +4.9%)

─────────────────────────────────────────────
🏆 Top 5 서비스

1. AmazonEC2           $78.00   +3.1% vs 어제    +2.5% vs 전월동기
2. AmazonS3            $20.00   -1.5% vs 어제    -0.8% vs 전월동기
3. AWSLambda            $9.00   ±0.0% vs 어제    +5.0% vs 전월동기
4. AmazonRDS            $8.00  +50.0% vs 어제 ⚠️  +1.2% vs 전월동기
5. AmazonCloudWatch     $4.00   +2.0% vs 어제    ±0.0% vs 전월동기
─────────────────────────────────────────────
💬 자세한 내용은 스레드에서 확인하세요.
```

---

## Main 1 — Thread 메시지 설계

> Main 1 아래에 순차 발송되는 스레드. 전체 비용 요약 및 서비스별 분석.

---

### Thread 1: hyu-ddps 계정의 모든 서비스 비용

**포함 정보:**
- 오늘 총비용
- 어제 총비용 + 차이 (금액 + %)

```
📋 hyu-ddps 계정 서비스 비용

오늘:    $123.45
어제:    $110.00   (+$13.45 / +12.2% 📈)
```

---

### Thread 2: IAM User별 비용 분석

> 각 IAM User가 해당 날짜에 사용한 총비용을 내림차순으로 나열하고, 각 User별로 비용이 높은 리소스를 드릴다운.
> 
> **NOTE (2026-04-09)**: IAM User별 비용에는 Tax 10%가 포함되어 있습니다.
> 이는 각 User가 실제로 부담해야 할 비용(Usage + Tax)을 정확히 반영하기 위함입니다.

**구성 요소:**
- 👤 IAM User 식별자 (태그 기반) + 해당 User의 일일 총비용 (내림차순 정렬) [세금 포함]
- 각 User 아래: 해당 User가 사용한 서비스 리소스를 비용 내림차순으로 리스트업
- 리소스명(서비스명) + 비용 표시 [세금 포함]
- 태그 미설정 리소스는 별도 항목으로 집계

```
👥 IAM User별 비용 분석

👤 kim@ddps.cloud              합계: $55.00
   ├─ AmazonEC2               $48.00
   ├─ AmazonS3                 $5.00
   └─ AmazonCloudWatch         $2.00

👤 park@ddps.cloud             합계: $40.00
   ├─ AmazonEC2               $30.00
   ├─ AmazonRDS                $8.00
   └─ AmazonS3                 $2.00

👤 lee@ddps.cloud              합계: $20.00
   ├─ AmazonEC2               $18.00
   └─ AWSLambda                $2.00

👤 (태그 없음 / 공용)           합계: $8.45
   ├─ AmazonCloudWatch         $4.00
   ├─ AWSLambda                $3.00
   └─ AmazonS3                 $1.45
```

---

### Thread 3: EC2 외 서비스 상세

> EC2를 제외한 모든 AWS 서비스의 상세 내역. 서비스별 총 비용을 내림차순으로 나열하고, 각 서비스 아래에 활성 region별 비용을 내림차순으로 표시.

**계층 구조:**
```
서비스명 + 총 비용
└── 활성 region (비용 높은 순)
    └── region별 비용
```

```
🔍 EC2 외 서비스 상세

📌 AmazonS3              $20.00   -1.5% vs 어제
   ├─ ap-northeast-2     $12.00
   ├─ us-east-1           $6.00
   └─ eu-west-1           $2.00

📌 AWSLambda              $9.00   ±0.0% vs 어제
   ├─ ap-northeast-2      $7.00
   └─ us-east-1           $2.00

📌 AmazonRDS              $8.00  +50.0% vs 어제 ⚠️
   └─ ap-northeast-2      $8.00

📌 AmazonCloudWatch       $4.00   +2.0% vs 어제
   ├─ ap-northeast-2      $2.50
   └─ us-east-1           $1.50

📌 AmazonVPC              $1.45   ±0.0% vs 어제
   └─ ap-northeast-2      $1.45
```

---

## Main 2 메시지 설계 — EC2 전용 리포트

> Main 1과 별개로 채널에 독립 발송되는 두 번째 메시지.
> EC2는 정보량이 많아 Main 1 스레드에 넣으면 가독성이 떨어지므로 별도 Main으로 분리.
> **Main 2도 핵심만 담는다**: 총 비용 + 활성 region + 비용 Top 5 인스턴스만.

### 구성 요소

- **헤더**: EC2 전용 리포트임을 명시 + 날짜
- **EC2 총 비용**: 전일 대비 금액 차이 + 변화율
- **활성 region 요약**: 비용이 발생한 region 목록 + region별 소계 (간결, 금액 내림차순)
- **Top 5 인스턴스 타입**: instance_type 기준 집계 비용 상위 5개
- **Thread 안내**: "전체 인스턴스 상세는 스레드에서 확인"

### Top 5 인스턴스 타입에 포함할 정보

| 항목 | 표시 이유 |
|------|----------|
| instance_type | 비용 단가 파악 (t3 vs p3 차이가 큼) |
| 인스턴스 수 | 해당 타입의 활성 인스턴스 개수 |
| 합산 비용 | 해당 타입 전체의 일 비용 합계 |

### Main 2 메시지 예시

```
🖥️ EC2 Instance Report — 2026-03-25
계정: hyu-ddps

EC2 총 비용: $78.00   (어제 $75.60 / +$2.40 / +3.1% 📈)

─────────────────────────────────────────────
📍 활성 Region

  ap-northeast-2 (서울)    $50.00
  us-east-1 (버지니아)      $28.00

─────────────────────────────────────────────
🏆 Top 5 인스턴스 타입 (비용 기준)

1. p3.2xlarge     1대    $28.00
2. g5.2xlarge     1대    $28.00
3. t3.large       1대    $15.00
4. g4dn.xlarge    1대     $4.00
5. t3.micro       1대     $2.00
─────────────────────────────────────────────
💬 전체 인스턴스 상세는 스레드에서 확인하세요.
```

---

## Main 2 — Thread 메시지 설계

> Main 2 아래에 순차 발송되는 스레드. Main 2에서 보여준 Top 5 외 전체 인스턴스를 포함한 상세 정보.
> region → 구매 유형 → 상태 → 인스턴스 단위로 드릴다운.

---

### Thread 1: 전체 인스턴스 상세

> Main 2에서 Top 5만 요약 표시했으므로, Thread에서는 모든 인스턴스를 계층 구조로 전부 표시.
> EC2 비용 규모가 가장 크고 변수가 많으므로 가장 상세한 계층 구조로 표시.

**계층 구조:**
```
EC2 총 비용
└── 활성 region (비용 높은 순)
    └── On-Demand / Spot 분류
        └── instance_state (running / stopped / terminated)
            └── 인스턴스별 정보
                - 비용
                - instance_name
                - instance_id
                - instance_type
                - 실행 시간 (h:m:s 포맷)
            └── 잔존 EBS / Snapshot 존재 여부 리스트
```

**`start` 상태 포함 여부:**
- `start`는 AWS 공식 instance_state값이 아니므로 **표시 대상에서 제외**
- 전환 중 과도 상태(`pending`, `stopping`, `shutting-down`)는 수명이 짧고 비용이 미미하므로 **별도 항목 없이 인접 상태에 귀속**
- **최종 표시 상태: `running` / `stopped` / `terminated` 3가지**

**조건부 DM 알림 발송 기준:**
- `stopped` 상태가 **24시간 이상** 경과한 인스턴스 → 해당 IAM User에게 DM 발송
- 잔존 EBS 또는 Snapshot이 존재 → 해당 IAM User에게 DM 발송

```
📌 AmazonEC2 총 비용: $78.00

┌─────────────────────────────────────────────────────
│ 📍 ap-northeast-2 (서울)   $50.00
├─────────────────────────────────────────────────────
│
│  [On-Demand]
│
│    ▶ running
│      🔸 $28.00  |  my-gpu-server       |  i-0a1b2c3d4e  |  p3.2xlarge  |  12:34:56
│          👤 park@ddps.cloud
│          잔존 리소스: EBS 200GB (gp3) — $10.00
│                      ⚠️ DM 알림 대상
│
│      🔸 $15.00  |  research-worker-01  |  i-0f1e2d3c4b  |  t3.large    |  08:12:00
│          👤 kim@ddps.cloud
│          잔존 리소스: 없음
│
│    ▶ stopped                             ⚠️ DM 알림 대상
│      🔸  $0.00  |  dev-instance-kim    |  i-0b2c3d4e5f  |  t3.medium   |  00:00:00
│          👤 kim@ddps.cloud
│          잔존 리소스: EBS 100GB (gp2)  — $5.00
│                      Snapshot 2개       — $0.80
│                      ⚠️ DM 알림 대상
│
│    ▶ terminated
│      🔸  $2.00  |  old-test-server     |  i-0c3d4e5f6a  |  t3.micro    |  04:20:00
│          👤 lee@ddps.cloud
│          잔존 리소스: Snapshot 1개      — $0.40
│                      ⚠️ DM 알림 대상
│
│  [Spot]
│
│    ▶ running
│      🔸  $4.00  |  spot-train-01       |  i-0d4e5f6a7b  |  g4dn.xlarge |  02:10:33
│          👤 lee@ddps.cloud
│          잔존 리소스: 없음
│
└─────────────────────────────────────────────────────

┌─────────────────────────────────────────────────────
│ 📍 us-east-1 (버지니아)    $28.00
├─────────────────────────────────────────────────────
│
│  [On-Demand]
│
│    ▶ running
│      🔸 $28.00  |  nlp-experiment      |  i-0e5f6a7b8c  |  g5.2xlarge  |  06:44:10
│          👤 park@ddps.cloud
│          잔존 리소스: 없음
│
└─────────────────────────────────────────────────────
```

#### IAM User 식별 방법

> `describe_instances()` 응답의 `OwnerId`는 AWS Account ID(12자리 숫자)이며 IAM User를 구별하지 않는다.
> 인스턴스를 실행한 IAM User는 **CloudTrail `lookup_events` + `RunInstances` 이벤트**를 통해 식별한다.

**조회 흐름:**
```
1. describe_instances() → 인스턴스 ID 목록 수집
2. CloudTrail lookup_events(EventName=RunInstances, 최대 60일 이내)
   → userIdentity.userName  = IAM username  (예: "kim")
   → Resources[].ResourceName = 인스턴스 ID (예: "i-0b2c3d4e5f")
3. 인스턴스 ID 매칭 → IAM username 확보
4. IAM username → IAM_SLACK_USER_MAP → Slack User ID → DM 발송
```

**제약 조건:**
- 이 시스템은 CloudTrail을 **최대 60일 이전**까지만 조회 (비용 및 실용성 고려)
- 60일 초과 인스턴스 → IAM User 표시: `"Unknown (60일 초과)"`
- 60일 초과 인스턴스에 대해서는 DM 발송 불가, Thread에만 `Unknown`으로 기재

---

### Thread 2: 미사용 리소스 목록

> 인스턴스에 연결되지 않은 EBS 볼륨과, AMI에서 참조되지 않는 Snapshot 목록.
> 비용 낭비를 유발하는 리소스를 인지하는 목적이며, 실제 삭제는 수행하지 않음.

---

#### 포함 대상 및 제외 기준

**미사용 EBS 볼륨** (`describe_volumes`, `status=available`)

| 구분 | 조건 | 비고 |
|------|------|------|
| 포함 | `available` 상태 (인스턴스 미연결) | Thread 5에 기재 ⚠️ |
| 제외 | Tags에 `kubernetes.io` 포함 (Kubernetes PVC 볼륨) | 생성 14일 초과 시에는 포함 ☄️ |
| 제외 | Tags에 `aws:backup:source-resource-arn` 포함 (AWS Backup 관리) | 전체 제외 |
| 제외 | 생성 후 1일 미만 | 프로비저닝 중일 가능성 있음 |

**미사용 EBS Snapshot** (`describe_snapshots`, `OwnerIds=[account_id]`)

| 구분 | 조건 | 표시 |
|------|------|------|
| 포함 | AMI 미참조 + 태그 없음 + `completed` | ⚠️ 명확한 정리 대상 |
| 포함 | AMI 미참조 + 태그 있음 + `completed` | ☄️ 확인 권장 수준 |
| 제외 | `describe_images()` 수집 AMI의 `BlockDeviceMappings.SnapshotId`에 포함 | 제외 — 삭제 시 AMI 손상 |
| 제외 | Tags에 `aws:backup:source-resource-arn` 포함 | 제외 |
| 제외 | Description에 `"Created by AWS Backup"` 포함 | 제외 |
| 제외 | State가 `pending` | 제외 — 생성 중 |

> 방치 기간이 **60일 이상**인 Snapshot은 🚨 으로 강조 표시

---

#### Thread 5 메시지 예시

```
[미사용 리소스 목록]

💾 미사용 EBS 볼륨 (3개)
  ap-northeast-2 | vol-0a1b2c3d | gp3 | 100GB | 45일간 미사용 ⚠️
  ap-northeast-2 | vol-0b2c3d4e | gp2 |  50GB | 20일간 미사용 ⚠️
  us-east-1      | vol-0c3d4e5f | gp3 | 200GB |  3일간 미사용 ☄️  (kubernetes)

📸 미사용 Snapshot (4개)
  ap-northeast-2 | snap-0a1b2c3d | 100GB | 72일간 미사용 🚨   ← 60일 이상 방치
  ap-northeast-2 | snap-0b2c3d4e |  50GB | 30일간 미사용 ⚠️
  ap-northeast-2 | snap-0c3d4e5f |  80GB | 15일간 미사용 ☄️   ← 태그 있음, 확인 권장
  us-east-1      | snap-0d4e5f6a |  20GB |  5일간 미사용 ⚠️
```

---

## 개인 DM 알림 설계

### IAM User 식별 및 Slack 매핑 전략

#### IAM User 식별

`describe_instances()` 응답만으로는 인스턴스를 실행한 IAM User를 알 수 없다.
**CloudTrail `lookup_events` + `RunInstances` 이벤트**로 IAM username을 역추적한다.

```
인스턴스 ID
  → CloudTrail lookup_events(EventName=RunInstances, 최대 60일 이내)
  → userIdentity.userName = IAM username  (예: "kim")
```

- CloudTrail 조회 범위: 최대 **60일 이전**까지
- 60일 초과 인스턴스: DM 발송 불가, Thread에 `"Unknown (60일 초과)"` 표시

#### IAM username → Slack User ID 매핑

IAM username과 Slack User ID는 **dict 형태의 매핑 테이블**로 관리한다.
Lambda 환경변수 `IAM_SLACK_USER_MAP`에 JSON으로 저장:

```json
{
  "kim":  "U0123ABC",
  "park": "U0456DEF",
  "lee":  "U0789GHI"
}
```

> **운영 주의사항**: 신규 연구원이 합류하거나 퇴사 시 이 매핑 테이블을 **수동으로 추가/삭제**해야 한다.
> 자동 동기화 메커니즘은 현재 없으며, 매핑 누락 시 해당 User에 대한 DM은 skip되고 Thread에 "매핑 없음" 로그가 기재된다.

---

### 발송 조건

| 조건 | DM 수신 대상 |
|------|-------------|
| EC2 인스턴스가 `stopped` 상태로 **24시간 이상** 경과한 경우 | CloudTrail 기반 IAM User |
| `stopped` 또는 `terminated` 인스턴스에 잔존 EBS가 있는 경우 | CloudTrail 기반 IAM User |
| `stopped` 또는 `terminated` 인스턴스에 잔존 Snapshot이 있는 경우 | CloudTrail 기반 IAM User |

> 동일 IAM User에게 여러 조건이 해당하는 경우, 하나의 DM으로 통합 발송.

---

### DM에 포함할 정보

- 대상 리소스 유형 (인스턴스 / EBS / Snapshot)
- 리소스 식별자 (instance_id, volume_id, snapshot_id 등)
- 리소스가 위치한 region
- 해당 리소스로 인해 발생 중인 일 비용
- 월 기준 절감 예상액 (일 비용 × 30)
- 권고 행동 (간결하게 1줄)

---

### DM 메시지 예시

**케이스 1 — stopped 인스턴스 + 잔존 EBS/Snapshot이 모두 있는 경우**

```
⚠️ [AWS 비용 알림] 낭비 리소스가 감지되었습니다 — 2026-03-25

안녕하세요, kim 님.
아래 리소스에서 불필요한 비용이 발생 중입니다.

─────────────────────────────────────────
🛑 정지된 EC2 인스턴스
   인스턴스:    dev-instance-kim (i-0b2c3d4e5f)
   타입:        t3.medium
   Region:     ap-northeast-2 (서울)
   상태:        stopped (36시간 경과)

💾 잔존 EBS 볼륨
   볼륨 ID:    vol-0a1b2c3d4e  (100GB / gp2)
   Region:     ap-northeast-2 (서울)
   일 비용:     $5.00

📸 잔존 Snapshot
   Snapshot:   snap-0f1e2d3c4b  /  snap-0e2f3a4b5c
   Region:     ap-northeast-2 (서울)
   일 비용:     $0.80

─────────────────────────────────────────
💡 위 리소스를 삭제하면 월 약 $174.00 절감이 가능합니다.
─────────────────────────────────────────
```

**케이스 2 — 잔존 Snapshot만 있는 경우 (terminated 인스턴스)**

```
⚠️ [AWS 비용 알림] 낭비 리소스가 감지되었습니다 — 2026-03-25

안녕하세요, lee 님.
아래 리소스에서 불필요한 비용이 발생 중입니다.

─────────────────────────────────────────
📸 잔존 Snapshot (terminated 인스턴스 연관)
   Snapshot:       snap-0c3d4e5f6a
   연관 인스턴스:   old-test-server (i-0c3d4e5f6a) — terminated
   Region:         ap-northeast-2 (서울)
   일 비용:         $0.40

─────────────────────────────────────────
💡 위 리소스를 삭제하면 월 약 $12.00 절감이 가능합니다.
─────────────────────────────────────────
```

---

## 리소스 삭제 조치 (미정)

> AMI 삭제, EBS Snapshot 삭제, EBS Volume 삭제를 Slack 인터페이스 내에서 어떻게 구현할 것인지 아직 결정되지 않았다.
>
> **고려 중인 조치 대상:**
> - disabled / deprecated AMI deregister
> - 미사용 EBS Snapshot 삭제 (`aws_delete_snapshot.py` 기존 로직 참고)
> - 미사용 EBS Volume 삭제
>
> **미결 질문:**
> - Slack Block Kit의 버튼(Button) 액션으로 삭제 요청을 트리거하는 방식이 적합한가?
> - 삭제 전 확인 단계(confirm dialog)를 어떻게 구성할 것인가?
> - 삭제 권한을 Lambda IAM Role에 부여하는 것이 보안 측면에서 적절한가?
> - 삭제 이력을 어디에 기록할 것인가? (CloudTrail만으로 충분한가?)

---

## 전체 메시지 흐름 다이어그램

```
[Slack 채널 — 매일 1회, 2개의 독립 메시지 순차 발송]
│
├── 📊 Main 1 — 전체 비용 요약
│     ├── 헤더: 날짜 + 계정명 + 일일 총비용
│     ├── 전일 대비 변화 (금액 + % + 📈/📉/➡️)
│     ├── Top 5 서비스 (전일 대비 % / 전월동기 대비 %)
│     │     └── 이상 서비스에 ⚠️ 표시
│     └── "자세한 내용은 스레드 참조" 안내
│
│     └── 💬 Main 1 Thread
│           ├── Thread 1: 계정 전체 요약
│           │     ├── 오늘 / 어제 비용 + 차이
│           │     ├── 7일 평균
│           │     └── 이번달 누계 vs 전월 동기
│           │
│           ├── Thread 2: IAM User별 비용 분석
│           │     ├── 👤 User-A  합계: $XX  (내림차순)
│           │     │     └── 서비스명 + 비용 (내림차순)
│           │     ├── 👤 User-B  합계: $XX
│           │     │     └── 서비스명 + 비용 (내림차순)
│           │     └── 👤 (태그 없음 / 공용)
│           │
│           ├── Thread 3: EC2 외 서비스 상세
│           │     └── 📌 서비스명 + 총 비용 (내림차순)
│           │               └── region별 비용 (내림차순)
│           │
│           └── Thread 4: 이상 감지 (조건 충족 시에만 발송)
│                 └── 이상 서비스 + 변화율 + 30일 평균 대비 + 관련 User
│
└── 🖥️ Main 2 — EC2 전용 리포트 (핵심만)
      ├── 헤더: EC2 전용 날짜
      ├── EC2 총 비용 + 전일 대비 변화
      ├── 활성 region 목록 + region별 소계 (내림차순)
      ├── Top 5 인스턴스 한 줄 요약
      │     └── instance_name | instance_type | AMI | IAM user | 비용 | 실행시간 | EBS 여부
      └── "전체 인스턴스 상세는 스레드 참조" 안내

      └── 💬 Main 2 Thread
            ├── Thread 1 (EC2): 전체 인스턴스 상세
            │     ├── IAM User 식별: CloudTrail RunInstances (최대 60일)
            │     │     └── 60일 초과 인스턴스: "Unknown (60일 초과)" 표시
            │     ├── EC2 총 비용
            │     └── 📍 region (비용 높은 순)
            │           ├── [On-Demand]
            │           │     ├── ▶ running    → 인스턴스 목록 + 잔존 리소스
            │           │     ├── ▶ stopped   → 인스턴스 목록 + 잔존 리소스 + ⚠️ DM 대상 (24h 이상)
            │           │     └── ▶ terminated → 인스턴스 목록 + 잔존 리소스
            │           └── [Spot]
            │                 ├── ▶ running
            │                 └── ▶ terminated
            │
            └── Thread 5 (EC2): 미사용 리소스 목록
                  ├── 💾 미사용 EBS 볼륨 목록 (kubernetes 14일↑, AWS Backup 제외)
                  └── 📸 미사용 Snapshot 목록 (AMI 참조, AWS Backup, pending 제외)
                        └── 60일 이상 방치 → 🚨 강조

[개인 DM — 조건 충족 시에만 발송]
│
└── ⚠️ DM (CloudTrail 기반 IAM User → IAM_SLACK_USER_MAP → Slack User ID)
      ├── 발송 조건 1: stopped 인스턴스 24시간 이상 경과
      ├── 발송 조건 2: 잔존 EBS 존재
      └── 발송 조건 3: 잔존 Snapshot 존재
            └── 리소스 식별자 + region + 일 비용 + 월 절감 예상액
      (매핑 없는 User: skip + Thread에 "매핑 없음" 로그 기재)
```

---

## 실서비스 인사이트 반영 테이블

| 서비스 | 참고한 아이디어 | 적용 위치 |
|--------|----------------|----------|
| **CloudZero** | Main은 요약만, Thread에 상세 분리 | 전체 구조 |
| **CloudZero** | "이상 감지" 시에만 강조 표시 | ⚠️ 마커, Thread 4 |
| **nOps** | 심각도 레벨 구분 (±5% / 20% 기준) | 변화율 아이콘 및 ⚠️ 기준 |
| **nOps** | IAM User @-mention으로 책임 소재 명시 | Main 1 Thread 2, Main 2 Thread 1의 👤 표시 |
| **nOps** | 당사자에게 직접 개인 알림 발송 | 개인 DM 알림 설계 전체 |
| **AWS Cost Explorer** | Usage Type으로 On-Demand / Spot 구분 | Main 2 Thread 1 EC2 상세 |
| **AWS Cost Explorer** | instance_state별 분류 | Main 2 Thread 1 running / stopped / terminated 계층 |
| **AWS Budgets** | 월 누계와 전월 동기 비교 | Thread 1 |
| **Kubecost** | 색상(아이콘) 기반 심각도 시각화 | 📈/📉/➡️/⚠️ 마커 |
| **Finout** | 모든 리소스를 동일 포맷으로 정규화 | Thread 4 서비스 통일 포맷 |
| **aws-billing-to-slack** | 지난달 vs 이번달 누계 비교 | Thread 1 |
| **내부 설계 신규** | IAM User별 총비용 집계 + 드릴다운 | Thread 2 전체 |
| **내부 설계 신규** | CloudTrail 기반 IAM User 역추적 | Main 2 Thread 1 IAM 식별, 개인 DM |
| **내부 설계 신규** | IAM username ↔ Slack User ID dict 매핑 (수동 관리) | 개인 DM IAM_SLACK_USER_MAP |
| **내부 설계 신규** | 잔존 EBS/Snapshot 발견 시 당사자 DM | 개인 DM 알림 설계 |
| **내부 설계 신규** | 미사용 EBS/Snapshot 별도 Thread 5로 분리 | Main 2 Thread 5 |
| **내부 설계 신규** | 단일 계정 기준 단순화 | Main 메시지 + Thread 1 구조 |
