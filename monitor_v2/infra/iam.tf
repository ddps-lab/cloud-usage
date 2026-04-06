# ── Lambda 실행 역할 ─────────────────────────────────────────────────
resource "aws_iam_role" "lambda_exec" {
  name        = "${local.function_name}-role"
  description = "monitor_v2 Lambda 실행 역할"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "LambdaAssumeRole"
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })

  tags = {
    Project = "cloud-usage-monitor"
    Version = "v2"
  }
}

# AWS 관리형 정책: CloudWatch Logs 기본 쓰기
resource "aws_iam_role_policy_attachment" "lambda_basic_exec" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# 커스텀 인라인 정책: 비즈니스 로직 권한
resource "aws_iam_policy" "monitor_v2" {
  name        = "${local.function_name}-policy"
  description = "monitor_v2가 필요한 AWS 서비스 조회 권한"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # ── Cost Explorer ───────────────────────────────────────────────
      # cost/data.py: fetch_daily_*, fetch_mtd_*, fetch_cost_forecast
      {
        Sid    = "CostExplorer"
        Effect = "Allow"
        Action = [
          "ce:GetCostAndUsage",
          "ce:GetCostForecast",
        ]
        Resource = "*"
      },
      # ── EC2 조회 ────────────────────────────────────────────────────
      # ec2/data.py: collect_instances, collect_unused_ebs,
      #              collect_unused_snapshots, DescribeRegions
      {
        Sid    = "EC2ReadOnly"
        Effect = "Allow"
        Action = [
          "ec2:DescribeInstances",
          "ec2:DescribeVolumes",
          "ec2:DescribeSnapshots",
          "ec2:DescribeImages",
          "ec2:DescribeRegions",
        ]
        Resource = "*"
      },
      # ── CloudTrail ──────────────────────────────────────────────────
      # ec2/iam_resolver.py: build_instance_creator_map (RunInstances 이벤트 조회)
      {
        Sid      = "CloudTrailReadOnly"
        Effect   = "Allow"
        Action   = ["cloudtrail:LookupEvents"]
        Resource = "*"
      },
      # ── STS ─────────────────────────────────────────────────────────
      # lambda_handler.py: sts.get_caller_identity()
      {
        Sid      = "STSGetCallerIdentity"
        Effect   = "Allow"
        Action   = ["sts:GetCallerIdentity"]
        Resource = "*"
      },
      # ── Athena ────────────────────────────────────
      # cost/data_cur.py, ec2/data_cur.py: Athena 쿼리 실행
      {
        Sid    = "AthenaQuery"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution",
        ]
        Resource = "*"
      },
      # ── Glue (Athena 메타스토어) ─────────────────────────────────────
      # Athena가 hyu_ddps_logs 데이터베이스 테이블 정보 조회 시 필요
      {
        Sid    = "GlueReadOnly"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetPartitions",
        ]
        Resource = "*"
      },
      # ── S3 (──────────────────────
      # Athena 쿼리 결과를 ATHENA_OUTPUT_LOCATION에 쓰고 읽는 권한
      # CUR 원본 파일이 저장된 버킷 읽기 권한
      {
        Sid    = "S3AthenaAccess"
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:PutObject",
        ]
        Resource = "*"
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "monitor_v2" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.monitor_v2.arn
}
