-- =============================================================================
-- Credit Risk DW — Snowflake Schema
-- Database: CREDIT_RISK_DW | Schema: STAGING
-- NOTE: This file is prepared but NOT yet executed. Switch DW_BACKEND=snowflake
--       and run this manually in Snowflake Console when ready to migrate.
-- =============================================================================

-- Infrastructure (run once via Terraform or Snowflake Console)
CREATE DATABASE IF NOT EXISTS CREDIT_RISK_DW;
CREATE SCHEMA IF NOT EXISTS CREDIT_RISK_DW.STAGING;
CREATE SCHEMA IF NOT EXISTS CREDIT_RISK_DW.MARTS;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE;

-- =============================================================================
-- STAGING LAYER
-- =============================================================================

CREATE TABLE IF NOT EXISTS CREDIT_RISK_DW.STAGING.STG_LOANS (
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
CLUSTER BY (DATA_YEAR, STATE_CODE);   -- Snowflake clustering key (replaces PG indexes)
