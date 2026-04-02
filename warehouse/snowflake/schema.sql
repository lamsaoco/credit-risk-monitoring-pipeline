-- =============================================================================
-- Credit Risk DW — Snowflake Schema
-- Database: CREDIT_RISK_DW
-- Schemas  : STG  (mirrors PostgreSQL 'credit_risk' schema → source for dbt)
--            PROD (mirrors PostgreSQL 'credit_risk_prod' schema → dbt marts output)
--
-- NOTE: Run this file ONCE manually in Snowflake Console before first pipeline run.
--       Switch pipeline by setting DW_BACKEND=snowflake in docker/.env
-- =============================================================================

-- Infrastructure (run once — idempotent)
CREATE DATABASE IF NOT EXISTS CREDIT_RISK_DW;

-- Schema names intentionally mirror dbt_project.yml +schema keys (stg / prod)
-- so that the generate_schema_name macro can resolve them correctly on Snowflake.
CREATE SCHEMA IF NOT EXISTS CREDIT_RISK_DW.STG;
CREATE SCHEMA IF NOT EXISTS CREDIT_RISK_DW.PROD;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE;

-- =============================================================================
-- STAGING LAYER — STG.STG_LOANS
-- Mirrors PostgreSQL credit_risk.stg_loans (source table for dbt staging model)
-- Clustering key replaces PostgreSQL declarative partitioning.
-- ID uses AUTOINCREMENT so stg_loans.sql can SELECT id without any change.
-- =============================================================================
CREATE TABLE IF NOT EXISTS CREDIT_RISK_DW.STG.STG_LOANS (
    -- Auto-generated surrogate key (mirrors PostgreSQL sequence-based id)
    ID                      NUMBER AUTOINCREMENT PRIMARY KEY,

    -- Identification
    ACTIVITY_YEAR           INT,
    LEI                     VARCHAR(20),
    STATE_CODE              VARCHAR(2),
    COUNTY_CODE             VARCHAR(5),
    CENSUS_TRACT            VARCHAR(11),

    -- Loan Details
    LOAN_TYPE               NUMBER(2),
    LOAN_PURPOSE            NUMBER(2),
    LOAN_AMOUNT             FLOAT,
    INTEREST_RATE           FLOAT,
    PROPERTY_VALUE          FLOAT,
    OCCUPANCY_TYPE          NUMBER(2),
    LIEN_STATUS             NUMBER(2),

    -- Borrower Financials
    INCOME                  INT,
    APPLICANT_AGE           VARCHAR(10),

    -- Computed Risk Metrics
    LOAN_TO_VALUE_RATIO     FLOAT,
    DEBT_TO_INCOME_NUMERIC  FLOAT,
    MKT_BENCHMARK           FLOAT,
    INTEREST_RATE_SPREAD    FLOAT,

    -- Metadata
    DATA_YEAR               INT NOT NULL,
    LOADED_AT               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (DATA_YEAR, STATE_CODE);  -- Replaces PostgreSQL LIST partitioning

-- =============================================================================
-- PROD SCHEMA — Populated by dbt build (no manual DDL needed here)
-- prd_risk_summary and prd_loan_sample are created as dbt-managed tables.
-- Documented below for reference only.
-- =============================================================================
-- CREATE TABLE IF NOT EXISTS CREDIT_RISK_DW.PROD.PRD_RISK_SUMMARY (...);
-- CREATE TABLE IF NOT EXISTS CREDIT_RISK_DW.PROD.PRD_LOAN_SAMPLE  (...);
