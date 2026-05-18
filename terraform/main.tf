provider "alicloud" {
  access_key = var.aliyun_access_key_id
  secret_key = var.aliyun_access_key_secret
  region     = var.region
}

variable "aliyun_access_key_id" {
  type        = string
  description = "阿里云 AccessKey ID"
}

variable "aliyun_access_key_secret" {
  type        = string
  sensitive   = true
  description = "阿里云 AccessKey Secret"
}

variable "region" {
  type        = string
  default     = "cn-hongkong"
  description = "阿里云地域"
}

# 数据湖存储 (OSS)
resource "alicloud_oss_bucket" "data_lake" {
  bucket = "techpulse-data-lake-hk-unique" # 名字需全局唯一
  acl    = "private"
}

# 大数据分析平台 (MaxCompute)
resource "alicloud_maxcompute_project" "tech_dw" {
  project_name  = "techpulse_dw"
  comment       = "TechPulse AI Warehouse in HK"
  
  # 关键点：设置默认配额
  # 按量付费模式下，通常默认为 "pay-as-you-go-default"
  default_quota = "os_PayAsYouGoQuota" 
  
  # 设置项目类型为按量付费
  product_type  = "PAYASYOUGO"
}
