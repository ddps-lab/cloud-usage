variable "aws_region" {
  description = "Lambda를 배포할 AWS 리전"
  type        = string
  default     = "ap-northeast-2"
}

variable "slack_bot_token" {
  description = "Slack Bot User OAuth Token (xoxb-...)"
  type        = string
  sensitive   = true
}

variable "slack_channel_id" {
  description = "메시지를 보낼 Slack 채널 ID (C로 시작)"
  type        = string
}

variable "account_name" {
  description = "리포트 헤더에 표시할 AWS 계정 별칭 (e.g., hyu-ddps)"
  type        = string
}

# ── CUR / Athena ─────────────────────────────────────────────

variable "athena_output_location" {
  description = "Athena 쿼리 결과를 저장할 S3 URI (e.g., s3://bucket/prefix/). CUR 미사용 시 빈 문자열"
  type        = string
  default     = ""
}

variable "athena_database" {
  description = "Athena 데이터베이스 이름"
  type        = string
  default     = "hyu_ddps_logs"
}

variable "athena_workgroup" {
  description = "Athena 워크그룹 이름"
  type        = string
  default     = "primary"
}

variable "athena_region" {
  description = "Athena 클라이언트 리전 (e.g., ap-northeast-2, us-east-1)"
  type        = string
  default     = "ap-northeast-2"
}

# ── Bedrock (AI 분석) ──────────────────────────────────────────────────

variable "bedrock_model_id" {
  description = "Bedrock에서 사용할 모델 ID (e.g., amazon.nova-micro-v1:0)"
  type        = string
  default     = "amazon.nova-micro-v1:0"
}

variable "bedrock_region" {
  description = "Bedrock을 지원하는 AWS 리전 (e.g., us-east-1)"
  type        = string
  default     = "us-east-1"
}

# ── Lambda 실행 설정 ────────────────────────────────────────────────

variable "lambda_timeout" {
  description = "Lambda 타임아웃 (초). 전 리전 순회 고려하여 최대값 권장"
  type        = number
  default     = 900 # 15분 (Lambda 최대값)
}

variable "lambda_memory_size" {
  description = "Lambda 메모리 크기 (MB)"
  type        = number
  default     = 512
}

# ── Main 4: hyperun 으로 낸 job ────────────────────────────────────────
#
# ★ 이 셋은 AWS 계정과 무관하다. Main 1~3 은 이 계정의 Cost Explorer 와 CUR 을 읽는데,
# Main 4 는 hyperun gateway 의 GET /v1/usage 하나만 부른다. 숫자의 출처가 PacsJob 이라
# vendor 별로 쪼갤 수 있고, 그래서 RunPod 이나 Shadeform 에서 쓴 돈도 여기서는 보인다.
# 셋 다 비어 있으면 Main 4 는 "설정이 없다" 고 말하고 아무것도 보내지 않는다.

variable "hyperun_api_base" {
  description = "hyperun gateway 주소. 예: https://api.hyperun.example.com (끝의 / 없이)"
  type        = string
  default     = ""
}

variable "hyperun_api_token" {
  description = <<-EOT
    hyperun 의 **operator 토큰**. /v1/usage 는 operator 전용이라 일반 토큰이면 403 이 온다.
    만료가 없는 static token 을 쓴다: 이 Lambda 는 브라우저가 없어서 로그인을 끝낼 수 없다.
  EOT
  type        = string
  sensitive   = true
  default     = ""
}

variable "hyperun_usage_days" {
  description = <<-EOT
    며칠치를 받을지. ★ 비워 두는 것이 정상이고, 그때 data.py 가 이달 1일부터 오늘까지로
    창을 잡는다 (days_this_month).

    숫자를 넣으면 그 규칙을 덮어쓰는데, 그러면 화면 안의 두 숫자가 어긋난다: 스레드 표의
    제목은 "이달 누계" 인데 창이 31일이면 5일에 돌았을 때 지난달 26일치가 그 표에 들어간다.
    지난달을 다시 보내야 할 때만 잠깐 넣고 도로 비운다.
  EOT
  type        = string
  default     = ""
}

