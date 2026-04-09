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

      # Python 의존성 설치 (slack-sdk는 순수 Python이므로 플랫폼 플래그 불필요)
      pip install slack-sdk \
        --target '${local.build_dir}' \
        --quiet

      # monitor_v2 패키지 복사 (infra/, 테스트 파일, 캐시 제외)
      cp -r '${local.project_root}/monitor_v2' '${local.build_dir}/monitor_v2'
      rm -rf '${local.build_dir}/monitor_v2/infra'
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
  description   = "monitor_v2: daily AWS cost + EC2 report to Slack"

  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

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
      BEDROCK_MODEL_ID       = var.bedrock_model_id
      BEDROCK_REGION         = var.bedrock_region
    }
  }

  tags = {
    Project = "cloud-usage-monitor"
    Version = "v2"
  }

  depends_on = [aws_cloudwatch_log_group.lambda_logs]
}

# ── EventBridge (CloudWatch Events) ──────────────────────────────────
#
# 스케줄 5개 (KST 기준, EventBridge는 UTC 사용)
#   KST 08:00 = UTC 23:00 전날  → cost  전날 데이터
#   KST 08:10 = UTC 23:10 전날  → ec2   전날 데이터
#   KST 08:15 = UTC 23:15 전날  → analysis  AI 비용 변화 분석 (Main 3)
#   KST 22:00 = UTC 13:00       → cost  당일 데이터
#   KST 22:10 = UTC 13:10       → ec2   당일 데이터

# ── KST 08:00 cost (전날) ─────────────────────────────────────────────
resource "aws_cloudwatch_event_rule" "morning_cost" {
  name                = "${local.function_name}-morning-cost"
  description         = "KST 08:00 cost report (yesterday)"
  schedule_expression = "cron(0 23 * * ? *)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "morning_cost" {
  rule      = aws_cloudwatch_event_rule.morning_cost.name
  target_id = "morning-cost"
  arn       = aws_lambda_function.monitor_v2.arn
  input     = jsonencode({ report_type = "cost", date_mode = "yesterday" })
}

resource "aws_lambda_permission" "morning_cost" {
  statement_id  = "AllowEventBridgeMorningCost"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.monitor_v2.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.morning_cost.arn
}

# ── KST 08:10 ec2 (전날) ──────────────────────────────────────────────
resource "aws_cloudwatch_event_rule" "morning_ec2" {
  name                = "${local.function_name}-morning-ec2"
  description         = "KST 08:10 ec2 report (yesterday)"
  schedule_expression = "cron(10 23 * * ? *)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "morning_ec2" {
  rule      = aws_cloudwatch_event_rule.morning_ec2.name
  target_id = "morning-ec2"
  arn       = aws_lambda_function.monitor_v2.arn
  input     = jsonencode({ report_type = "ec2", date_mode = "yesterday" })
}

resource "aws_lambda_permission" "morning_ec2" {
  statement_id  = "AllowEventBridgeMorningEc2"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.monitor_v2.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.morning_ec2.arn
}

# ── KST 08:15 analysis (Main 3: AI 비용 변화 분석) ──────────────────────
resource "aws_cloudwatch_event_rule" "morning_analysis" {
  name                = "${local.function_name}-morning-analysis"
  description         = "KST 08:15 AI cost analysis report (yesterday)"
  schedule_expression = "cron(15 23 * * ? *)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "morning_analysis" {
  rule      = aws_cloudwatch_event_rule.morning_analysis.name
  target_id = "morning-analysis"
  arn       = aws_lambda_function.monitor_v2.arn
  input     = jsonencode({ report_type = "analysis", date_mode = "yesterday" })
}

resource "aws_lambda_permission" "morning_analysis" {
  statement_id  = "AllowEventBridgeMorningAnalysis"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.monitor_v2.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.morning_analysis.arn
}

# ── KST 22:00 cost (당일) ─────────────────────────────────────────────
resource "aws_cloudwatch_event_rule" "evening_cost" {
  name                = "${local.function_name}-evening-cost"
  description         = "KST 22:00 cost report (today)"
  schedule_expression = "cron(0 13 * * ? *)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "evening_cost" {
  rule      = aws_cloudwatch_event_rule.evening_cost.name
  target_id = "evening-cost"
  arn       = aws_lambda_function.monitor_v2.arn
  input     = jsonencode({ report_type = "cost", date_mode = "today" })
}

resource "aws_lambda_permission" "evening_cost" {
  statement_id  = "AllowEventBridgeEveningCost"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.monitor_v2.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.evening_cost.arn
}

# ── KST 22:10 ec2 (당일) ──────────────────────────────────────────────
resource "aws_cloudwatch_event_rule" "evening_ec2" {
  name                = "${local.function_name}-evening-ec2"
  description         = "KST 22:10 ec2 report (today)"
  schedule_expression = "cron(10 13 * * ? *)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "evening_ec2" {
  rule      = aws_cloudwatch_event_rule.evening_ec2.name
  target_id = "evening-ec2"
  arn       = aws_lambda_function.monitor_v2.arn
  input     = jsonencode({ report_type = "ec2", date_mode = "today" })
}

resource "aws_lambda_permission" "evening_ec2" {
  statement_id  = "AllowEventBridgeEveningEc2"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.monitor_v2.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.evening_ec2.arn
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
