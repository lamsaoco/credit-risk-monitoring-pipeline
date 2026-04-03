terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    # Snowflake provider — managed by snowflakedb
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 0.100"
    }
  }
}

# ─────────────────────────────────────────────────────────────────────────── #
# AWS Provider                                                                #
# ─────────────────────────────────────────────────────────────────────────── #
provider "aws" {
  region = var.aws_region
}

# ─────────────────────────────────────────────────────────────────────────── #
# Snowflake Provider                                                          #
# Even when enable_snowflake = false, Terraform requires the provider to be   #
# successfully configured. We pass credentials via terraform.tfvars.          #
# ─────────────────────────────────────────────────────────────────────────── #
provider "snowflake" {
  # New provider version requires splitting the account string (e.g. ORG-ACCOUNT)
  organization_name = length(split("-", var.snowflake_account)) > 1 ? split("-", var.snowflake_account)[0] : ""
  account_name      = length(split("-", var.snowflake_account)) > 1 ? split("-", var.snowflake_account)[1] : var.snowflake_account
  user              = var.snowflake_user
  password          = var.snowflake_password
  role              = var.snowflake_role
}


# ─────────────────────────────────────────────────────────────────────────── #
# AWS — S3 Data Lake                                                          #
# ─────────────────────────────────────────────────────────────────────────── #
resource "aws_s3_bucket" "data_lake" {
  bucket        = var.bucket_name
  force_destroy = true

  tags = {
    Name        = var.bucket_name
    Project     = var.project_name
    Environment = "Dev"
  }
}

resource "aws_s3_bucket_public_access_block" "block_public" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_object" "folders" {
  for_each = toset(["raw", "staging", "prod"])

  bucket = aws_s3_bucket.data_lake.id
  key    = "${each.value}/"
}

# ─────────────────────────────────────────────────────────────────────────── #
# Snowflake — Data Warehouse Infrastructure                                   #
# All resources use count = var.enable_snowflake ? 1 : 0 so they are         #
# completely skipped when running PostgreSQL mode. Idempotent by design.      #
# ─────────────────────────────────────────────────────────────────────────── #

# Database
resource "snowflake_database" "credit_risk_dw" {
  count = var.enable_snowflake ? 1 : 0
  name  = "CREDIT_RISK_DW"
}

# RAW schema — source layer for raw data from Airflow
resource "snowflake_schema" "raw" {
  count    = var.enable_snowflake ? 1 : 0
  database = snowflake_database.credit_risk_dw[0].name
  name     = "RAW"

  depends_on = [snowflake_database.credit_risk_dw]
}

# STG schema — output layer for dbt staging models
resource "snowflake_schema" "stg" {
  count    = var.enable_snowflake ? 1 : 0
  database = snowflake_database.credit_risk_dw[0].name
  name     = "STG"

  depends_on = [snowflake_database.credit_risk_dw]
}

# PROD schema — output layer for dbt mart models (prd_risk_summary, prd_loan_sample)
resource "snowflake_schema" "prod" {
  count    = var.enable_snowflake ? 1 : 0
  database = snowflake_database.credit_risk_dw[0].name
  name     = "PROD"

  depends_on = [snowflake_database.credit_risk_dw]
}

# Compute Warehouse — X-SMALL auto-suspend for cost control
resource "snowflake_warehouse" "compute_wh" {
  count          = var.enable_snowflake ? 1 : 0
  name           = "COMPUTE_WH"
  warehouse_size = "X-SMALL"
  auto_suspend   = 120
  auto_resume    = true
}

# STG_LOANS table — mirrors PostgreSQL credit_risk.stg_loans
# ID uses identity (Snowflake AUTOINCREMENT) so dbt stg_loans.sql SELECT id works unchanged.
# CLUSTER BY (DATA_YEAR, STATE_CODE) replaces PostgreSQL LIST partitioning for query performance.
resource "snowflake_table" "stg_loans" {
  count    = var.enable_snowflake ? 1 : 0
  database = snowflake_schema.raw[0].database
  schema   = snowflake_schema.raw[0].name
  name     = "STG_LOANS"

  # Auto-increment primary key (mirrors PostgreSQL stg_loans_id_seq)
  column {
    name = "ID"
    type = "NUMBER(38,0)"
    identity {
      start_num = 1
      step_num  = 1
    }
  }
  column {
    name = "ACTIVITY_YEAR"
    type = "NUMBER(38,0)"
  }
  column {
    name = "LEI"
    type = "VARCHAR(20)"
  }
  column {
    name = "STATE_CODE"
    type = "VARCHAR(2)"
  }
  column {
    name = "COUNTY_CODE"
    type = "VARCHAR(5)"
  }
  column {
    name = "CENSUS_TRACT"
    type = "VARCHAR(11)"
  }
  column {
    name = "LOAN_TYPE"
    type = "NUMBER(2,0)"
  }
  column {
    name = "LOAN_PURPOSE"
    type = "NUMBER(2,0)"
  }
  column {
    name = "LOAN_AMOUNT"
    type = "FLOAT"
  }
  column {
    name = "INTEREST_RATE"
    type = "FLOAT"
  }
  column {
    name = "PROPERTY_VALUE"
    type = "FLOAT"
  }
  column {
    name = "OCCUPANCY_TYPE"
    type = "NUMBER(2,0)"
  }
  column {
    name = "LIEN_STATUS"
    type = "NUMBER(2,0)"
  }
  column {
    name = "INCOME"
    type = "NUMBER(38,0)"
  }
  column {
    name = "APPLICANT_AGE"
    type = "VARCHAR(10)"
  }
  column {
    name = "LOAN_TO_VALUE_RATIO"
    type = "FLOAT"
  }
  column {
    name = "DEBT_TO_INCOME_NUMERIC"
    type = "FLOAT"
  }
  column {
    name = "MKT_BENCHMARK"
    type = "FLOAT"
  }
  column {
    name = "INTEREST_RATE_SPREAD"
    type = "FLOAT"
  }
  column {
    name     = "DATA_YEAR"
    type     = "NUMBER(38,0)"
    nullable = false
  }
  column {
    name = "LOADED_AT"
    type = "TIMESTAMP_NTZ(9)"
    default { expression = "CURRENT_TIMESTAMP()" }
  }

  # Clustering key replaces PostgreSQL LIST partitioning (DATA_YEAR / STATE_CODE)
  cluster_by = ["DATA_YEAR", "STATE_CODE"]

  depends_on = [snowflake_schema.raw]
}

# ─────────────────────────────────────────────────────────────────────────── #
# Outputs                                                                     #
# ─────────────────────────────────────────────────────────────────────────── #
output "s3_bucket_name" {
  description = "S3 data lake bucket name"
  value       = aws_s3_bucket.data_lake.id
}

output "snowflake_database" {
  description = "Snowflake database name (null when enable_snowflake = false)"
  value       = var.enable_snowflake ? snowflake_database.credit_risk_dw[0].name : null
}

output "snowflake_stg_schema" {
  description = "Snowflake STG schema (null when enable_snowflake = false)"
  value       = var.enable_snowflake ? snowflake_schema.stg[0].name : null
}