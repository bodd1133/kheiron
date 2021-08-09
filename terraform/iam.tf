data "aws_iam_policy_document" "assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

# ECS Execution role
resource "aws_iam_role" "execution" {
  name                 = "${local.name}-execution-role"
  assume_role_policy   = data.aws_iam_policy_document.assume_role_policy.json
}

resource "aws_iam_role_policy" "execution" {
  name   = "${local.name}-execution-policy"
  role   = aws_iam_role.execution.id
  policy = data.aws_iam_policy_document.execution_policy_document.json
}

data "aws_iam_policy_document" "execution_policy_document" {

  statement {
    actions = [
      "ecr:GetAuthorizationToken",
      "ecr:BatchCheckLayerAvailability",
      "ecr:GetDownloadUrlForLayer",
      "ecr:BatchGetImage",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["*"]
  }
  statement {
    sid    = "AllowSecretsManager"
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
      "secretsmanager:DescribeSecret",
      "secretsmanager:ListSecrets",
      "secretsmanager:ListSecretVersionIds"
    ]
    resources = ["*"]
  }
}

# ECS task role
resource "aws_iam_role" "task" {
  name                 = "${local.name}-task-role"
  assume_role_policy   = data.aws_iam_policy_document.assume_role_policy.json
}

resource "aws_iam_role_policy" "task" {
  name   = "${local.name}-task-policy"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task.json
}

data "aws_iam_policy_document" "task" {
  statement {
    actions = [
      "cloudwatch:List*",
      "cloudwatch:Get*",
    ]

    resources = ["*"]
  }
  statement {
    sid = "DescribeRegions"
    actions = [
      "ec2:DescribeRegions"
    ]
    resources = ["*"]
  }
  statement {
    sid = "AccessS3"
    actions = [
      "s3:ListBucket",
      "s3:GetObject"
    ]
    resources = [
      aws_s3_bucket.data.arn
    ]
  }
}