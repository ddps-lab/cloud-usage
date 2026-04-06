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

