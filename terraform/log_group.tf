resource "aws_cloudwatch_log_group" "analysis" {
  name              = "/aws/ecs/${var.environment}"
  retention_in_days = 7
}