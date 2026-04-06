terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

locals {
  function_name = "monitor-v2-daily-report"
  project_root  = "${path.module}/../.."
  build_dir     = "${path.module}/.build"
}

# ── 의존성 설치 & 소스 복사 ──────────────────────────────────────────
#
# pip install 결과와 monitor_v2 소스를 .build/ 디렉토리에 모아서
# archive_file이 단일 zip으로 묶을 수 있게 준비한다.
#
# 트리거:
#   - pyproject.toml 변경 → 의존성 재설치
#   - monitor_v2/**/*.py 변경 → 소스 재복사
resource "null_resource" "build_package" {
  triggers = {
    requirements = filemd5("${local.project_root}/pyproject.toml")
    source_hash = sha256(join(",", [
      for f in sort(fileset("${local.project_root}/monitor_v2", "**/*.py")) :
      filesha256("${local.project_root}/monitor_v2/${f}")
      if !startswith(f, "test_")
    ]))
  }

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      rm -rf '${local.build_dir}'
      mkdir -p '${local.build_dir}'

      # Python 의존성 설치 (Lambda 런타임 타겟)
      pip install slack-sdk \
        --target '${local.build_dir}' \
        --quiet \
        --platform manylinux2014_x86_64 \
        --only-binary=:all: \
        --python-version 3.12

      # monitor_v2 패키지 복사 (테스트 파일 제외)
      cp -r '${local.project_root}/monitor_v2' '${local.build_dir}/monitor_v2'
      find '${local.build_dir}/monitor_v2' -name 'test_*.py' -delete
      find '${local.build_dir}/monitor_v2' -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true
    EOT
    interpreter = ["bash", "-c"]
  }
}

data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = local.build_dir
  output_path = "${path.module}/dist/lambda.zip"
  depends_on  = [null_resource.build_package]
}

# ── Lambda 함수 ──────────────────────────────────────────────────────
resource "aws_lambda_function" "monitor_v2" {
  function_name = local.function_name
  description   = "monitor_v2: 일일 AWS 비용 + EC2 현황 → Slack"

  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  # monitor_v2/__init__.py 가 있으므로 패키지 경로로 지정
  handler = "monitor_v2.lambda_handler.lambda_handler"
  runtime = "python3.12"

  role        = aws_iam_role.lambda_exec.arn
  timeout     = var.lambda_timeout
  memory_size = var.lambda_memory_size

  environment {
    variables = {
      SLACK_BOT_TOKEN        = var.slack_bot_token
      SLACK_CHANNEL_ID       = var.slack_channel_id
      ACCOUNT_NAME           = var.account_name
      ATHENA_OUTPUT_LOCATION = var.athena_output_location
      ATHENA_DATABASE        = var.athena_database
      ATHENA_WORKGROUP       = var.athena_workgroup
    }
  }

  tags = {
    Project = "cloud-usage-monitor"
    Version = "v2"
  }

  depends_on = [aws_cloudwatch_log_group.lambda_logs]
}

# ── EventBridge (CloudWatch Events) ──────────────────────────────────
resource "aws_cloudwatch_event_rule" "daily_trigger" {
  name                = "${local.function_name}-trigger"
  description         = "monitor_v2 일일 실행 트리거 (KST 22:00)"
  schedule_expression = var.schedule_expression
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.daily_trigger.name
  target_id = "monitor-v2-lambda"
  arn       = aws_lambda_function.monitor_v2.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.monitor_v2.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_trigger.arn
}

# ── CloudWatch Logs ───────────────────────────────────────────────────
resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${local.function_name}"
  retention_in_days = 30

  tags = {
    Project = "cloud-usage-monitor"
    Version = "v2"
  }
}
