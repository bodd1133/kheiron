resource "aws_ecs_task_definition" "analysis" {
  family                   = local.name
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]

  container_definitions = data.template_file.analysis_task_definition.rendered

  execution_role_arn = aws_iam_role.execution.arn
  task_role_arn      = aws_iam_role.task.arn

  cpu    = 2048
  memory = 8192
}

data "template_file" "analysis_task_definition" {
  template = file("task_definitions/analyse_uni_data.json")

  vars = {
    name             = local.name
    s3_bucket = aws_s3_bucket.data.id
    docker_image     = aws_ecr_repository.analysis.repository_url
    docker_image_tag = "latest"
    log_group        = aws_cloudwatch_log_group.analysis.name
    region        = var.region
  }
}