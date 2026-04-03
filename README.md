<div align="center">

# 🛡️ Credit Risk Monitoring Pipeline

**An end-to-end Data Engineering project for mortgage credit risk analysis across the United States**

[![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)](https://python.org)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE?logo=apacheairflow)](https://airflow.apache.org)
[![dbt](https://img.shields.io/badge/dbt-1.9-FF694B?logo=dbt)](https://getdbt.com)
[![Snowflake](https://img.shields.io/badge/Snowflake-Cloud%20DW-29B5E8?logo=snowflake)](https://snowflake.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql)](https://postgresql.org)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-PySpark-E25A1C?logo=apachespark)](https://spark.apache.org)
[![Terraform](https://img.shields.io/badge/Terraform-IaC-7B42BC?logo=terraform)](https://terraform.io)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit)](https://streamlit.io)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)](https://docker.com)
[![EC2](https://img.shields.io/badge/AWS%20EC2-m7i--flex.large-FF9900?logo=amazonec2)](https://aws.amazon.com/ec2/instance-types/m7i/)

</div>

---

## 📋 Table of Contents

- [The Real-World Problem](#-the-real-world-problem)
- [Solution Architecture](#-solution-architecture)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Data Pipeline Overview](#-data-pipeline-overview)
- [Data Sources](#-data-sources)
- [dbt Transformation Layer](#-dbt-transformation-layer)
- [Data Warehouse Architecture](#-data-warehouse-architecture)
- [Dashboard](#-dashboard)
- [Infrastructure as Code](#-infrastructure-as-code)
- [Prerequisites](#-prerequisites)
- [Quick Start (Step-by-Step)](#-quick-start-step-by-step)
- [Backend Switching (PostgreSQL ↔ Snowflake)](#-backend-switching-postgresql--snowflake)
- [Airflow DAG Reference](#-airflow-dag-reference)
- [Environment Variables Reference](#-environment-variables-reference)

---

## 🏦 The Real-World Problem

### Context

Every year, U.S. financial institutions originate over **10 million retail mortgage loans**, generating a massive publicly available dataset maintained by the **Consumer Financial Protection Bureau (CFPB)** under the *Home Mortgage Disclosure Act (HMDA)*.

This dataset contains granular information on loan amounts, applicant income, debt-to-income ratios, interest rates, property values, geographic identifiers, and loan outcomes — making it one of the most comprehensive public lending records in the world.

### The Challenge

Despite the data being publicly available, **mortgage risk officers, analysts, and regulators face several critical operational challenges:**

1. **Data Volume:** The raw HMDA dataset contains **12+ million rows per year**, stored as compressed ZIP files in proprietary CSV format. Direct querying is impractical without an ETL pipeline.

2. **No Risk Segmentation:** The raw data lacks computed risk indicators. There is no ready-made `loan-to-value ratio`, `debt-to-income spread`, or `market benchmark comparison` — these must be derived by joining with macroeconomic indicators (e.g., the Federal Reserve's Federal Funds Rate).

3. **No Historical View:** Analyzing credit risk trends over multiple years requires ingesting, cleaning, and normalizing data across different annual snapshots into a unified model.

4. **Operational Blindspot:** Without a monitoring dashboard, risk officers cannot quickly answer questions like:
   - *Which U.S. states have the highest concentration of high-risk loans?*
   - *How is the average LTV ratio trending against the market benchmark rate?*
   - *What % of loans in California are classified as "High Risk" vs. "Low Risk"?*

### Our Solution

This project builds a **production-grade, fully automated data engineering pipeline** that:

- Ingests 12M+ HMDA mortgage records and Federal Reserve interest rate data annually
- Enriches raw data with computed risk features via **PySpark** in a distributed processing step
- Loads the enriched dataset into a **dual-backend Data Warehouse** (PostgreSQL for local/dev, Snowflake for cloud/production) — switchable with a single environment variable
- Transforms raw warehouse data into analytical **dbt marts** with risk segmentation logic
- Serves a **real-time Streamlit dashboard** allowing filtering by year, state, risk segment, and loan type

---

## 🏗️ Solution Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        CREDIT RISK MONITORING PIPELINE                          │
│                                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────────────────┐  │
│  │  DATA LAKE   │    │  PROCESSING  │    │         DATA WAREHOUSE           │  │
│  │   (AWS S3)   │    │  (PySpark)   │    │                                  │  │
│  │              │    │              │    │  ┌────────────┐  ┌────────────┐  │  │
│  │  raw/        │───▶│  Enrich:     │───▶│  │ PostgreSQL │  │ Snowflake  │  │  │
│  │  ├─hmda/     │    │  - LTV Ratio │    │  │  (local)   │  │  (cloud)   │  │  │
│  │  └─fred_raw/ │    │  - DTI Score │    │  └────────────┘  └────────────┘  │  │
│  │  staging/    │    │  - Mkt Rate  │    │         ▲               ▲         │  │
│  │  └─enriched/ │    │  - Spread    │    │         │               │         │  │
│  └──────────────┘    └──────────────┘    │  ┌──────┴───────────────┴──────┐  │  │
│                                          │  │       dbt (Transform)        │  │  │
│  ┌──────────────────────────────────┐    │  │  STG Views → PROD Marts     │  │  │
│  │       ORCHESTRATION              │    │  └──────────────────────────────┘  │  │
│  │       (Apache Airflow)           │    └──────────────────────────────────┘  │
│  │                                  │                                           │
│  │  DAG 1: Ingest ─────────────────▶ DAG 2: Transform ──────────────────────  │
│  │                                       │                                      │
│  │  DAG 3: Load to DW ◀─────────────────┘                                      │
│  │          │                                                                   │
│  │  DAG 4: dbt Build ◀───────────────────────────────────────────────────────  │
│  └──────────────────────────────────┘                                           │
│                                                                                 │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │                    VISUALIZATION (Streamlit)                              │  │
│  │         US Risk Heatmap · KPI Metrics · Loan Distribution Charts         │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
```
graph LR
    subgraph Infrastructure
        TF[Terraform] --> AWS[AWS S3/EC2]
    end

    subgraph Orchestration
        AF[Apache Airflow]
    end

    subgraph Data_Lake[Data Lake - S3]
        R[Raw Layer]
        S[Staging Layer]
    end

    subgraph Processing[Compute]
        SP[PySpark / Spark]
    end

    subgraph Warehouse[Storage & Modeling]
        PG[(Postgres)]
        SF[(Snowflake)]
        DBT[[dbt - Transform]]
    end

    subgraph BI[Visualization]
        ST[Streamlit App]
    end

    %% Flow
    R --> SP
    SP -- Enrichment --> S
    S -- Chunk Load --> PG & SF
    PG & SF --- DBT
    DBT -- Prod Marts --> ST

    %% Airflow control
    AF -.-> SP
    AF -.-> PG
    AF -.-> DBT
---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Orchestration** | Apache Airflow 2.x | DAG scheduling, pipeline automation, DAG chaining |
| **Ingestion** | Python + `boto3` + `fredapi` | HMDA ZIP download, FRED API pull, S3 upload |
| **Processing** | PySpark (local mode) | Feature engineering on 12M+ row datasets |
| **Data Lake** | AWS S3 | Raw and staging data storage (Parquet format) |
| **Data Warehouse** | PostgreSQL 15 / Snowflake | Dual-backend DW with single-switch toggle |
| **Transformation** | dbt 1.9 | SQL-based modeling, data quality tests, seeds |
| **IaC** | Terraform | AWS S3 + Snowflake DB/Schema/Table/Warehouse provisioning |
| **Dashboard** | Streamlit | Interactive risk monitoring UI |
| **Containerization** | Docker Compose | Fully containerized local development environment |
| **Compute** | AWS EC2 `m7i-flex.large` | 2 vCPU · 8 GB RAM · Production execution host |
| **Language** | Python 3.12 | All pipeline logic and DAGs |

---

## 📁 Project Structure

```
credit-risk-monitoring-pipeline/
│
├── airflow/
│   ├── dags/
│   │   ├── ingest_raw_data.py          # DAG 1: FRED API + HMDA download → S3
│   │   ├── transform_data.py           # DAG 2: PySpark enrichment → S3 staging
│   │   ├── load_to_warehouse.py        # DAG 3: S3 staging → PostgreSQL / Snowflake
│   │   └── dbt_transform_mart.py       # DAG 4: dbt build (staging views + prod marts)
│   └── airflow_variables.json.example  # Template for Airflow Variables
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml             # Source definition pointing to RAW schema
│   │   │   └── stg_loans.sql           # Staging view over RAW.STG_LOANS
│   │   └── marts/
│   │       ├── fct_loan_risk.sql       # Fact table: risk segmentation + lookups
│   │       ├── prd_risk_summary.sql    # Aggregate: state-level risk summary
│   │       └── prd_loan_sample.sql     # Sample table for dashboard scatter plots
│   ├── seeds/                          # Lookup CSVs: loan_type, state_mapping, etc.
│   ├── macros/
│   │   └── generate_schema_name.sql    # Custom schema routing (STG / PROD / LOOKUP)
│   ├── dbt_project.yml
│   └── profiles.yml                    # Connection profiles for dev + snowflake targets
│
├── dashboard/
│   ├── app.py                          # Streamlit application (Corporate Trust theme)
│   ├── requirements.txt
│   ├── Dockerfile
│   └── .streamlit/
│       └── config.toml                 # Streamlit theme configuration
│
├── terraform/
│   ├── main.tf                         # Snowflake: DB, RAW/STG/PROD schemas, COMPUTE_WH, STG_LOANS
│   ├── variables.tf
│   ├── terraform.tfvars.example        # Template — copy to terraform.tfvars
│   └── .terraform.lock.hcl
│
├── docker/
│   ├── docker-compose.yml              # Full stack: Airflow, dbt, Dashboard, Postgres, pgAdmin
│   ├── Dockerfile                      # Airflow worker image with all dependencies
│   ├── .env.example                    # Config template — copy to .env
│   └── .env                            # Local secrets (gitignored)
│
└── warehouse/
    └── postgres/
        └── schema.sql                  # PostgreSQL DW schema (partitioned tables)
```

---

## 🔄 Data Pipeline Overview

The pipeline is composed of **4 chained Airflow DAGs**. Triggering DAG 1 automatically cascades through the entire chain:

```
DAG 1: credit_risk_master_ingestion
   ├── ingest_fred_api          # Pull Fed Funds Rate from FRED API → S3
   ├── download_hmda_data       # Download HMDA ZIP from CFPB → local
   ├── process_hmda_spark       # PySpark CSV → Parquet (schema enforcement)
   ├── upload_hmda_s3           # Upload cleaned Parquet → S3 raw/
   ├── cleanup_hmda_local       # Free up local disk space
   └── trigger_transform ──────▶ DAG 2

DAG 2: mortgage_risk_staging_enrichment
   ├── validate_raw_mortgage_landing  # Assert S3 files exist before processing
   ├── download_raw_to_local_boto3    # Download HMDA + FRED Parquet → local
   ├── enrich_mortgage_features_spark # Join + compute LTV, DTI, Market Spread
   ├── upload_staging_to_s3           # Upload enriched Parquet → S3 staging/
   ├── cleanup_staging_local          # Free up local disk space
   └── trigger_load_warehouse ────────▶ DAG 3

DAG 3: credit_risk_load_warehouse
   ├── validate_staging_landing       # Assert staging files exist
   ├── download_staging_to_local      # Download enriched Parquet → local
   ├── load_to_warehouse              # Load → PostgreSQL OR Snowflake (based on DW_BACKEND)
   └── trigger_dbt_marts ─────────────▶ DAG 4

DAG 4: credit_risk_dbt_marts
   ├── dbt_debug    # Verify DW connection
   ├── dbt_seed     # Load lookup CSV seeds
   ├── dbt_deps     # Install dbt packages
   └── dbt_build    # Run staging views + mart models + data quality tests
```

> **💡 Key Design:** DAGs 2, 3, and 4 have `schedule_interval=None`. They are exclusively triggered by their upstream DAG via `TriggerDagRunOperator`, preventing duplicate runs.

---

## 📊 Data Sources

### 1. HMDA (Home Mortgage Disclosure Act)
- **Provider:** Consumer Financial Protection Bureau (CFPB)
- **URL:** `https://files.ffiec.cfpb.gov/dynamic-data/{year}/{year}_lar.zip`
- **Volume:** ~12 million rows per year
- **Format:** ZIP-compressed CSV → converted to Parquet via PySpark
- **Key Fields:** `state_code`, `loan_type`, `loan_amount`, `interest_rate`, `property_value`, `income`, `applicant_age`, `debt_to_income_ratio`

### 2. FRED (Federal Reserve Economic Data)
- **Provider:** Federal Reserve Bank of St. Louis
- **Series:** `FEDFUNDS` — Effective Federal Funds Rate (monthly)
- **Purpose:** Market benchmark for interest rate spread calculation
- **API:** Free API key at [fred.stlouisfed.org](https://fred.stlouisfed.org)

---

## 🔧 dbt Transformation Layer

### Schema Architecture

```
Snowflake:                        PostgreSQL:
┌─────────────────────────┐       ┌───────────────────────────────┐
│ RAW.STG_LOANS           │       │ credit_risk.stg_loans         │
│ (Physical table, raw)   │       │ (Partitioned by data_year)    │
└────────────┬────────────┘       └──────────────┬────────────────┘
             │                                   │
             ▼                                   ▼
┌─────────────────────────┐       ┌───────────────────────────────┐
│ STG.stg_loans (View)    │       │ stg.stg_loans (View)          │
└────────────┬────────────┘       └──────────────┬────────────────┘
             │                                   │
             ▼                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PROD / prod schema                           │
│  ┌─────────────────────┐   ┌────────────────┐   ┌───────────┐  │
│  │ fct_loan_risk       │   │ prd_risk_      │   │ prd_loan_ │  │
│  │ (Full fact table)   │   │ summary        │   │ sample    │  │
│  └─────────────────────┘   └────────────────┘   └───────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Risk Segmentation Logic (in `fct_loan_risk.sql`)

| Risk Segment | Condition |
|---|---|
| **High** | `LTV > 80%` OR `DTI > 43%` |
| **Medium** | `LTV between 60–80%` AND `DTI ≤ 43%` |
| **Low** | All other cases |

### Cost Category Logic

| Category | Condition |
|---|---|
| **High Cost** | `interest_rate_spread > 0` (above market benchmark) |
| **Market/Subsidized** | `interest_rate_spread ≤ 0` |

---

## 🏛️ Data Warehouse Architecture

The project supports **two interchangeable DW backends**, controlled by a single environment variable:

### PostgreSQL (Local / Development)
- Dedicated container `postgres-dw` (port 5433)
- Schema: `credit_risk`
- Table: `credit_risk.stg_loans` — **LIST-partitioned by `data_year`**
- Predicate pushdown via partition pruning for year-filtered queries
- dbt target: `dev`

### Snowflake (Cloud / Production)
- Provisioned via Terraform (see [IaC section](#-infrastructure-as-code))
- Database: `CREDIT_RISK_DW`
- Schemas: `RAW` (raw ingestion), `STG` (dbt staging), `PROD` (dbt marts), `LOOKUP` (seeds)
- Table: `RAW.STG_LOANS` — **clustered by `(DATA_YEAR, STATE_CODE)`**
- Auto-suspend warehouse (`COMPUTE_WH`, X-SMALL, 120s)
- dbt target: `snowflake`

---

## 📈 Dashboard

The Streamlit dashboard (port `8501`) provides:

| Widget | Description |
|---|---|
| **Year Filter** | Dropdown populated from live DW query |
| **State Filter** | Multi-select for all 50 U.S. states |
| **KPI Cards** | Total loans, Avg LTV, Avg DTI, Avg interest rate |
| **US Heatmap** | Choropleth map of loan count by state |
| **Risk Distribution** | Pie chart: High / Medium / Low risk breakdown |
| **Loan Type Breakdown** | Bar chart by conventional, FHA, VA, USDA |
| **LTV vs. DTI Scatter** | Sample scatter colored by risk segment |

**Theme:** Corporate Trust — Navy Blue (`#1D3557`) + Gold (`#B38D4F`) + clean white background, powered by `Playfair Display` + `Roboto` typefaces from Google Fonts.

---

## 🌐 Infrastructure as Code

Both **AWS S3** and **Snowflake** infrastructure are fully managed by Terraform:

```hcl
# AWS — S3 Data Lake (always provisioned)
- aws_s3_bucket.data_lake                   # Private S3 bucket (force_destroy=true)
- aws_s3_bucket_public_access_block         # Blocks all public access
- aws_s3_object["raw", "staging", "prod"]   # Pre-creates the 3 Data Lake layer prefixes

# Snowflake — Data Warehouse (only when enable_snowflake = true)
- snowflake_database.credit_risk_dw
- snowflake_schema.raw        # Landing zone for raw ingested data
- snowflake_schema.stg        # dbt staging views output
- snowflake_schema.prod       # dbt mart views output
- snowflake_schema.lookup     # dbt seed tables
- snowflake_warehouse.compute_wh   # X-SMALL, auto_suspend=120s
- snowflake_table.stg_loans        # Clustered by (DATA_YEAR, STATE_CODE)
```

Toggle Snowflake provisioning with `enable_snowflake = true/false` in `terraform.tfvars`.  
S3 is always provisioned regardless of the DW backend setting.

---

## 🖥️ Deployment Environment

This entire project was developed and tested on an **AWS EC2 instance**:

| Attribute | Value |
|---|---|
| **Instance Type** | `m7i-flex.large` |
| **vCPUs** | 2 |
| **RAM** | 8 GB |
| **OS** | Ubuntu 22.04 LTS |
| **Region** | `ap-southeast-1` (Singapore) |
| **Storage** | EBS gp3 |

All Docker containers (Airflow, dbt, PostgreSQL, Streamlit) run concurrently on this single instance. PySpark is tuned to operate within the 8 GB RAM constraint:
- Spark driver: 4 GB
- Shuffle partitions: 50 (reduced from default 200)
- Processing 12M+ rows via local Spark mode without a cluster

> **💡 Replication Tip:** Any machine with 8+ GB RAM and Docker installed can run this project. The EC2 `m7i-flex.large` is the minimum recommended spec for processing full yearly HMDA datasets.

---

## ✅ Prerequisites

Before you begin, ensure you have the following installed and configured:

| Requirement | Version | Notes |
|---|---|---|
| **Docker Desktop / Engine** | 24+ | With Docker Compose v2 |
| **Git** | Any | For cloning the repository |
| **AWS CLI** | v2 | Configured with credentials (`aws configure`) |
| **AWS S3 Bucket** | — | Pre-created; used as the Data Lake |
| **FRED API Key** | — | Free at [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html) |
| **Terraform** | 1.5+ | Only required for Snowflake backend |
| **Snowflake Account** | — | Only required for Snowflake backend |

---

## 🚀 Quick Start (Step-by-Step)

### Step 1 — Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/credit-risk-monitoring-pipeline.git
cd credit-risk-monitoring-pipeline
```

### Step 2 — Configure Environment Variables

```bash
cp docker/.env.example docker/.env
```

Open `docker/.env` and fill in your values:

```dotenv
# Required for all backends
AIRFLOW__CORE__FERNET_KEY=        # Generate: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
AIRFLOW__WEBSERVER__SECRET_KEY=   # Any random string
AWS_ACCESS_KEY_ID=                # Your AWS Access Key (or leave empty if using IAM role on EC2)
AWS_SECRET_ACCESS_KEY=            # Your AWS Secret Key
AIRFLOW_VAR_S3_BUCKET_NAME=       # Your S3 bucket name
AIRFLOW_VAR_FRED_API_KEY=         # Your FRED API key
AIRFLOW_VAR_EMAIL_RECEIVER=       # Alert email address

# Set your absolute project path (important for Docker-in-Docker volume mounting)
HOST_PROJECT_ROOT=/home/ubuntu/credit-risk-monitoring-pipeline
```

> **💡 Backend Selection:** By default `DW_BACKEND=postgresql`. To use Snowflake, see the [Backend Switching](#-backend-switching-postgresql--snowflake) section first.

### Step 3 — Build and Start All Services

```bash
cd docker
docker compose up -d --build
```

This starts:
- **Airflow Webserver** → http://localhost:8080 (admin/admin)
- **Airflow Scheduler**
- **PostgreSQL** (Airflow metadata DB)
- **PostgreSQL DW** (port 5433)
- **pgAdmin** → http://localhost:5050
- **Streamlit Dashboard** → http://localhost:8501
- **dbt Docs** → http://localhost:8090

Wait ~60 seconds for Airflow to finish initializing.

### Step 4 — Configure Airflow Variables

In the Airflow UI (http://localhost:8080), go to **Admin → Variables → Import Variables** and upload:

```bash
# First copy the example file and fill in your values
cp airflow/airflow_variables.json.example /tmp/airflow_variables.json
# Edit the file with your actual values, then import via UI
```

Or set them manually in the UI:

| Key | Value |
|---|---|
| `s3_bucket_name` | Your S3 bucket name |
| `fred_api_key` | Your FRED API key |
| `email_receiver` | Alert destination email |

### Step 5 — Initialize the PostgreSQL Data Warehouse Schema

```bash
docker exec -i docker-postgres-dw-1 psql -U dw_user -d credit_risk_dw < warehouse/postgres/schema.sql
```

### Step 6 — Trigger the Pipeline

In the Airflow UI:

1. Navigate to **DAGs**
2. Enable the DAG: `credit_risk_master_ingestion`
3. Click **▶ Trigger DAG** and set the year parameter:
   ```json
   { "year": 2024 }
   ```
4. Watch the pipeline automatically cascade:
   - **DAG 1** (Ingest) completes → **DAG 2** (Transform) starts
   - **DAG 2** completes → **DAG 3** (Load to DW) starts
   - **DAG 3** completes → **DAG 4** (dbt Build) starts

### Step 7 — View the Dashboard

Once DAG 4 finishes, open http://localhost:8501 to explore the Credit Risk Monitoring Dashboard.

> **💡 Cache Note:** If the Year/State dropdowns don't show new data immediately, press `C` on the keyboard inside the Streamlit page to clear the cache.

---

## 🔄 Backend Switching (PostgreSQL ↔ Snowflake)

### Option A: PostgreSQL (Default)

```dotenv
# docker/.env
DW_BACKEND=postgresql
DBT_ADAPTER=postgres
DBT_TARGET=dev
```

No additional setup required. Run Step 3 above.

### Option B: Snowflake (Cloud Production)

#### Step B1 — Provision Snowflake Infrastructure (One-time)

```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
```

Edit `terraform/terraform.tfvars`:

```hcl
enable_snowflake   = true
snowflake_account  = "your_account.ap-southeast-1"
snowflake_user     = "your_username"
snowflake_password = "your_password"
snowflake_role     = "SYSADMIN"
bucket_name        = "your-s3-bucket"
aws_region         = "ap-southeast-1"
project_name       = "credit-risk"
```

```bash
cd terraform
terraform init
terraform apply
```

This provisions: `CREDIT_RISK_DW` database · `RAW/STG/PROD/LOOKUP` schemas · `COMPUTE_WH` warehouse · `RAW.STG_LOANS` table.

#### Step B2 — Switch the Backend

Update `docker/.env`:

```dotenv
DW_BACKEND=snowflake
DBT_ADAPTER=snowflake
DBT_TARGET=snowflake

SNOWFLAKE_ACCOUNT=your_account.ap-southeast-1
SNOWFLAKE_USER=your_snowflake_user
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_DATABASE=CREDIT_RISK_DW
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_ROLE=SYSADMIN
```

#### Step B3 — Restart Services

```bash
cd docker
docker compose up -d
```

Airflow will automatically construct the `snowflake_default` connection from the environment variables — no UI configuration needed.

---

## 📋 Airflow DAG Reference

| DAG ID | Schedule | Triggers | Description |
|---|---|---|---|
| `credit_risk_master_ingestion` | `@yearly` (manual trigger recommended) | → DAG 2 | FRED API pull + HMDA download + Spark ETL |
| `mortgage_risk_staging_enrichment` | `None` (triggered by DAG 1) | → DAG 3 | PySpark feature engineering |
| `credit_risk_load_warehouse` | `None` (triggered by DAG 2) | → DAG 4 | S3 staging → DW (PostgreSQL or Snowflake) |
| `credit_risk_dbt_marts` | `None` (triggered by DAG 3) | — | dbt build: staging views + mart models |

**DAG Parameters:**

All 4 DAGs accept a `year` parameter (integer, default: `2024`):

```json
{ "year": 2024 }
```

---

## 🔐 Environment Variables Reference

| Variable | Required | Description |
|---|---|---|
| `AIRFLOW__CORE__FERNET_KEY` | ✅ | Airflow encryption key |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | ✅ | Airflow web session key |
| `AWS_ACCESS_KEY_ID` | ⚠️ | AWS credentials (optional if using IAM role) |
| `AWS_SECRET_ACCESS_KEY` | ⚠️ | AWS credentials (optional if using IAM role) |
| `AWS_DEFAULT_REGION` | ✅ | AWS region (e.g. `ap-southeast-1`) |
| `AIRFLOW_VAR_S3_BUCKET_NAME` | ✅ | S3 bucket used as Data Lake |
| `AIRFLOW_VAR_FRED_API_KEY` | ✅ | FRED API key for interest rate data |
| `AIRFLOW_VAR_EMAIL_RECEIVER` | ✅ | Email for pipeline failure alerts |
| `DW_BACKEND` | ✅ | `postgresql` or `snowflake` |
| `DBT_ADAPTER` | ✅ | `postgres` or `snowflake` |
| `DBT_TARGET` | ✅ | `dev` or `snowflake` |
| `HOST_PROJECT_ROOT` | ✅ | Absolute path to this repo on your host machine |
| `SNOWFLAKE_ACCOUNT` | ❄️ | Snowflake account identifier |
| `SNOWFLAKE_USER` | ❄️ | Snowflake username |
| `SNOWFLAKE_PASSWORD` | ❄️ | Snowflake password |
| `SNOWFLAKE_DATABASE` | ❄️ | Target database (default: `CREDIT_RISK_DW`) |
| `SNOWFLAKE_WAREHOUSE` | ❄️ | Virtual warehouse (default: `COMPUTE_WH`) |

> ⚠️ = Optional depending on setup · ❄️ = Required only for Snowflake backend

---

## 👨‍💻 Author

**PhanNH** — Data Engineering Project

*Built with Apache Airflow · PySpark · dbt · Snowflake · Streamlit · Docker · Terraform*
