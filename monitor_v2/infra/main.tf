terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0, < 5.100.0"
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
    command     = <<-EOT
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
    # ★ merge 로 쓰는 이유는 하나다: HYPERUN_USAGE_DAYS 는 보통 아예 없어야 한다.
    # 빈 문자열로 넣어도 코드는 같게 동작하지만, 콘솔에서 환경변수를 읽는 사람에게
    # 빈 값은 "설정이 안 됐나" 로 읽힌다. 없으면 없는 것이다.
    variables = merge({
      SLACK_BOT_TOKEN        = var.slack_bot_token
      SLACK_CHANNEL_ID       = var.slack_channel_id
      ACCOUNT_NAME           = var.account_name
      ATHENA_OUTPUT_LOCATION = var.athena_output_location
      ATHENA_DATABASE        = var.athena_database
      ATHENA_WORKGROUP       = var.athena_workgroup
      ATHENA_REGION          = var.athena_region
      BEDROCK_MODEL_ID       = var.bedrock_model_id
      BEDROCK_REGION         = var.bedrock_region

      # Main 4 (hyperun). 셋 다 비어 있으면 Main 4 만 조용히 쉬고 나머지는 그대로 돈다.
      #
      # ★ environment.variables 는 map 전체가 하나의 값이라 terraform 이 여기 있는 키를
      # 전부 맞춘다. 콘솔에서 손으로 넣은 값이 있으면 apply 가 그것을 지운다. plan 의
      # `Plan: 1 to change` 는 리소스 수이지 속성 수가 아니므로, apply 전에 `~` 줄을
      # 하나씩 읽는다.
      HYPERUN_API_BASE  = var.hyperun_api_base
      HYPERUN_API_TOKEN = var.hyperun_api_token
      },
      var.hyperun_usage_days == "" ? {} : { HYPERUN_USAGE_DAYS = var.hyperun_usage_days },
    )
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
#   KST 22:00 = UTC 13:00       → cost  전날 데이터
#   KST 22:10 = UTC 13:10       → ec2   전날 데이터
#   KST 22:15 = UTC 13:15       → analysis  전날 AI 비용 변화 분석

# ── KST 22:00 cost (전날) ─────────────────────────────────────────────
resource "aws_cloudwatch_event_rule" "evening_cost" {
  name                = "${local.function_name}-evening-cost"
  description         = "KST 22:00 cost report (yesterday)"
  schedule_expression = "cron(0 13 * * ? *)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "evening_cost" {
  rule      = aws_cloudwatch_event_rule.evening_cost.name
  target_id = "evening-cost"
  arn       = aws_lambda_function.monitor_v2.arn
  input     = jsonencode({ report_type = "cost", date_mode = "yesterday" })
}

resource "aws_lambda_permission" "evening_cost" {
  statement_id  = "AllowEventBridgeEveningCost"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.monitor_v2.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.evening_cost.arn
}

# ── KST 22:10 ec2 (전날) ──────────────────────────────────────────────
resource "aws_cloudwatch_event_rule" "evening_ec2" {
  name                = "${local.function_name}-evening-ec2"
  description         = "KST 22:10 ec2 report (yesterday)"
  schedule_expression = "cron(10 13 * * ? *)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "evening_ec2" {
  rule      = aws_cloudwatch_event_rule.evening_ec2.name
  target_id = "evening-ec2"
  arn       = aws_lambda_function.monitor_v2.arn
  input     = jsonencode({ report_type = "ec2", date_mode = "yesterday" })
}

resource "aws_lambda_permission" "evening_ec2" {
  statement_id  = "AllowEventBridgeEveningEc2"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.monitor_v2.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.evening_ec2.arn
}

# ── KST 22:15 analysis (AI 비용 변화 분석, 전날) ──────────────────────
resource "aws_cloudwatch_event_rule" "evening_analysis" {
  name                = "${local.function_name}-evening-analysis"
  description         = "KST 22:15 AI cost analysis report (yesterday)"
  schedule_expression = "cron(15 13 * * ? *)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "evening_analysis" {
  rule      = aws_cloudwatch_event_rule.evening_analysis.name
  target_id = "evening-analysis"
  arn       = aws_lambda_function.monitor_v2.arn
  input     = jsonencode({ report_type = "analysis", date_mode = "yesterday" })
}

resource "aws_lambda_permission" "evening_analysis" {
  statement_id  = "AllowEventBridgeEveningAnalysis"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.monitor_v2.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.evening_analysis.arn
}

# ── KST 22:20 hyperun (Main 4, 당일) ──────────────────────────────────
#
# ★ 앞의 셋과 달리 "전날" 이 아니라 당일이다. 끝난 하루가 아니라 지금 돌고 있는 job 이
# 얼마를 쓰고 있는지가 이 보고서의 질문이고, KST 22:20 은 UTC 13:20 이라 그 날이 13 시간
# 지난 시점이다. 끝난 날은 스레드의 일별 표에 그대로 남는다.
#
# date_mode 를 넘기지 않는다. Main 4 는 AWS 를 안 보므로 날짜를 고를 일이 없고,
# report.py 가 UTC 오늘을 기준일로 쓴다.
resource "aws_cloudwatch_event_rule" "evening_hyperun" {
  name                = "${local.function_name}-evening-hyperun"
  description         = "KST 22:20 hyperun job report (today, month to date)"
  schedule_expression = "cron(20 13 * * ? *)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "evening_hyperun" {
  rule      = aws_cloudwatch_event_rule.evening_hyperun.name
  target_id = "evening-hyperun"
  arn       = aws_lambda_function.monitor_v2.arn
  input     = jsonencode({ report_type = "hyperun" })
}

resource "aws_lambda_permission" "evening_hyperun" {
  statement_id  = "AllowEventBridgeEveningHyperun"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.monitor_v2.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.evening_hyperun.arn
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
