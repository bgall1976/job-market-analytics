variable "project_name" {
  description = "Name of the project, used as prefix for all resources"
  type        = string
  default     = "job-analytics"
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "data_retention_days" {
  description = "Days to retain data in bronze layer before transitioning to cheaper storage"
  type        = number
  default     = 30
}

variable "enable_versioning" {
  description = "Enable S3 versioning (disable for free tier optimization)"
  type        = bool
  default     = false
}

variable "glue_crawler_schedule" {
  description = "Cron schedule for Glue crawlers (empty = on-demand only)"
  type        = string
  default     = ""  # On-demand to save costs
}
