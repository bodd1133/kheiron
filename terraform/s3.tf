resource "aws_s3_bucket" "data" {
    bucket = "${local.name}-data"
    acl    = "private"
}

locals {
    files_to_upload = [ "../data/uni1.csv", "../data/uni2.csv", "../data/uni3.json" ]
}

resource "aws_s3_bucket_object" "data" {
    for_each = toset(local.files_to_upload)
    bucket = aws_s3_bucket.data.id
    key = each.value
    source = each.value
}