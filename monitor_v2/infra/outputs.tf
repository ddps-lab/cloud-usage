output "lambda_function_name" {
  description = "Lambda 함수 이름"
  value       = aws_lambda_function.monitor_v2.function_name
}

output "lambda_function_arn" {
  description = "Lambda 함수 ARN"
  value       = aws_lambda_function.monitor_v2.arn
}

output "lambda_invoke_arn" {
  description = "Lambda 호출 ARN (API Gateway 연동 등에 사용)"
  value       = aws_lambda_function.monitor_v2.invoke_arn
}

output "lambda_role_arn" {
  description = "Lambda 실행 역할 ARN"
  value       = aws_iam_role.lambda_exec.arn
}

output "eventbridge_rule_names" {
  description = "EventBridge 규칙 이름 목록 (4개 스케줄)"
  value = {
    morning_cost = aws_cloudwatch_event_rule.morning_cost.name
    morning_ec2  = aws_cloudwatch_event_rule.morning_ec2.name
    evening_cost = aws_cloudwatch_event_rule.evening_cost.name
    evening_ec2  = aws_cloudwatch_event_rule.evening_ec2.name
  }
}

output "log_group_name" {
  description = "CloudWatch 로그 그룹 이름"
  value       = aws_cloudwatch_log_group.lambda_logs.name
}
