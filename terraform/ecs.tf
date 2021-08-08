resource "aws_ecs_cluster" "analysis" {
  name = "${local.name}-ecs-cluster"
}

resource "aws_ecs_service" "analysis" {
  name             = "${local.name}-ecs-service"
  cluster          = aws_ecs_cluster.analysis.id
  task_definition  = aws_ecs_task_definition.analysis.arn
  desired_count    = 1
  launch_type      = "FARGATE"
  platform_version = "1.4.0"

   network_configuration {
    subnets         = var.subnets
    assign_public_ip = true
  }
}