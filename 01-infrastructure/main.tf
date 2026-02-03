# =============================================================================
# DATA LAKE S3 BUCKETS
# =============================================================================

# Generate unique suffix for globally unique bucket names
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

locals {
  bucket_suffix = random_id.bucket_suffix.hex
  common_tags = {
    Project = var.project_name
  }
}

# -----------------------------------------------------------------------------
# Bronze Layer - Raw Data
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "bronze" {
  bucket = "${var.project_name}-bronze-${local.bucket_suffix}"

  tags = merge(local.common_tags, {
    Layer = "bronze"
    Description = "Raw ingested data"
  })
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Disabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    id     = "transition-to-ia"
    status = "Enabled"

    transition {
      days          = var.data_retention_days
      storage_class = "STANDARD_IA"
    }

    expiration {
      days = 90  # Delete after 90 days to stay in free tier
    }

    filter {
      prefix = ""
    }
  }
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# -----------------------------------------------------------------------------
# Silver Layer - Cleaned Data
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "silver" {
  bucket = "${var.project_name}-silver-${local.bucket_suffix}"

  tags = merge(local.common_tags, {
    Layer = "silver"
    Description = "Cleaned and validated data"
  })
}

resource "aws_s3_bucket_versioning" "silver" {
  bucket = aws_s3_bucket.silver.id
  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Disabled"
  }
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket = aws_s3_bucket.silver.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# -----------------------------------------------------------------------------
# Gold Layer - Aggregated Data
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "gold" {
  bucket = "${var.project_name}-gold-${local.bucket_suffix}"

  tags = merge(local.common_tags, {
    Layer = "gold"
    Description = "Business-ready aggregated data"
  })
}

resource "aws_s3_bucket_versioning" "gold" {
  bucket = aws_s3_bucket.gold.id
  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Disabled"
  }
}

resource "aws_s3_bucket_public_access_block" "gold" {
  bucket = aws_s3_bucket.gold.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# =============================================================================
# AWS GLUE DATA CATALOG
# =============================================================================

resource "aws_glue_catalog_database" "main" {
  name        = replace("${var.project_name}_${var.environment}", "-", "_")
  description = "Data catalog for ${var.project_name} job market analytics"
}

# -----------------------------------------------------------------------------
# Glue Crawlers for Schema Discovery
# -----------------------------------------------------------------------------

resource "aws_glue_crawler" "bronze" {
  name          = "${var.project_name}-bronze-crawler"
  database_name = aws_glue_catalog_database.main.name
  role          = aws_iam_role.glue_crawler.arn

  s3_target {
    path = "s3://${aws_s3_bucket.bronze.bucket}/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })

  # Only set schedule if provided (empty = on-demand)
  schedule = var.glue_crawler_schedule != "" ? var.glue_crawler_schedule : null

  tags = local.common_tags
}

resource "aws_glue_crawler" "silver" {
  name          = "${var.project_name}-silver-crawler"
  database_name = aws_glue_catalog_database.main.name
  role          = aws_iam_role.glue_crawler.arn

  s3_target {
    path = "s3://${aws_s3_bucket.silver.bucket}/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  schedule = var.glue_crawler_schedule != "" ? var.glue_crawler_schedule : null

  tags = local.common_tags
}

resource "aws_glue_crawler" "gold" {
  name          = "${var.project_name}-gold-crawler"
  database_name = aws_glue_catalog_database.main.name
  role          = aws_iam_role.glue_crawler.arn

  s3_target {
    path = "s3://${aws_s3_bucket.gold.bucket}/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  schedule = var.glue_crawler_schedule != "" ? var.glue_crawler_schedule : null

  tags = local.common_tags
}

# =============================================================================
# S3 BUCKET FOLDER STRUCTURE
# =============================================================================

# Create folder structure in each bucket
resource "aws_s3_object" "bronze_folders" {
  for_each = toset(["job_postings/", "company_info/", "skills_data/"])

  bucket = aws_s3_bucket.bronze.id
  key    = each.value
  source = "/dev/null"
}

resource "aws_s3_object" "silver_folders" {
  for_each = toset(["jobs_cleaned/", "companies/", "skills_extracted/"])

  bucket = aws_s3_bucket.silver.id
  key    = each.value
  source = "/dev/null"
}

resource "aws_s3_object" "gold_folders" {
  for_each = toset(["fact_job_postings/", "dim_companies/", "dim_skills/", "dim_locations/", "aggregates/"])

  bucket = aws_s3_bucket.gold.id
  key    = each.value
  source = "/dev/null"
}
