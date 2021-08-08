variable "environment" {
    type = string
    description= "namespace to deploy infrastructure into"
    default = "rlb"
}

variable "region" {
    type = string
    description= "aws region to deply infra into"
    default = "eu-west-1"
}

variable "subnets" {
    type = list(string)
    description= "list of subnets to deploy cs onto"
    default = ["subnet-1af4d840", "subnet-dc55b6a5"]
}
