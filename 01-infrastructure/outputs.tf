# =============================================================================
# OUTPUTS
# Export values for use in other pipeline components
# =============================================================================

# -----------------------------------------------------------------------------
# S3 Buckets
# -----------------------------------------------------------------------------
output "s3_bucket_bronze" {
  description = "Bronze layer S3 bucket name"
  value       = aws_s3_bucket.bronze.bucket
}

output "s3_bucket_bronze_arn" {
  description = "Bronze layer S3 bucket ARN"
  value       = aws_s3_bucket.bronze.arn
}

output "s3_bucket_silver" {
  description = "Silver layer S3 bucket name"
  value       = aws_s3_bucket.silver.bucket
}

output "s3_bucket_silver_arn" {
  description = "Silver layer S3 bucket ARN"
  value       = aws_s3_bucket.silver.arn
}

output "s3_bucket_gold" {
  description = "Gold layer S3 bucket name"
  value       = aws_s3_bucket.gold.bucket
}

output "s3_bucket_gold_arn" {
  description = "Gold layer S3 bucket ARN"
  value       = aws_s3_bucket.gold.arn
}

# -----------------------------------------------------------------------------
# Glue Catalog
# -----------------------------------------------------------------------------
output "glue_database_name" {
  description = "Glue catalog database name"
  value       = aws_glue_catalog_database.main.name
}

output "glue_crawler_bronze" {
  description = "Bronze layer Glue crawler name"
  value       = aws_glue_crawler.bronze.name
}

output "glue_crawler_silver" {
  description = "Silver layer Glue crawler name"
  value       = aws_glue_crawler.silver.name
}

output "glue_crawler_gold" {
  description = "Gold layer Glue crawler name"
  value       = aws_glue_crawler.gold.name
}

# -----------------------------------------------------------------------------
# IAM Roles
# -----------------------------------------------------------------------------
output "glue_crawler_role_arn" {
  description = "IAM role ARN for Glue crawlers"
  value       = aws_iam_role.glue_crawler.arn
}

output "data_pipeline_role_arn" {
  description = "IAM role ARN for data pipeline services"
  value       = aws_iam_role.data_pipeline.arn
}

output "developer_user_name" {
  description = "IAM user name for local development"
  value       = aws_iam_user.developer.name
}

# -----------------------------------------------------------------------------
# S3 Paths (for use in configs)
# -----------------------------------------------------------------------------
output "s3_paths" {
  description = "S3 paths for pipeline configuration"
  value = {
    bronze_job_postings = "s3://${aws_s3_bucket.bronze.bucket}/job_postings/"
    bronze_company_info = "s3://${aws_s3_bucket.bronze.bucket}/company_info/"
    silver_jobs         = "s3://${aws_s3_bucket.silver.bucket}/jobs_cleaned/"
    silver_companies    = "s3://${aws_s3_bucket.silver.bucket}/companies/"
    gold_facts          = "s3://${aws_s3_bucket.gold.bucket}/fact_job_postings/"
    gold_dimensions     = "s3://${aws_s3_bucket.gold.bucket}/"
  }
}

# -----------------------------------------------------------------------------
# Configuration Export (JSON)
# -----------------------------------------------------------------------------
output "pipeline_config" {
  description = "Complete configuration for pipeline components"
  value = {
    aws_region    = var.aws_region
    project_name  = var.project_name
    environment   = var.environment
    
    buckets = {
      bronze = aws_s3_bucket.bronze.bucket
      silver = aws_s3_bucket.silver.bucket
      gold   = aws_s3_bucket.gold.bucket
    }
    
    glue = {
      database = aws_glue_catalog_database.main.name
      crawlers = {
        bronze = aws_glue_crawler.bronze.name
        silver = aws_glue_crawler.silver.name
        gold   = aws_glue_crawler.gold.name
      }
    }
  }
}
