-- =============================================================================
-- Credit Risk DW — PostgreSQL Schema (Declarative Partitioning)
-- Database: credit_risk_dw
-- Partition strategy:
--   Level 1 (parent)  : PARTITION BY LIST (data_year)
--   Level 2 (per year): PARTITION BY LIST (state_code)
--   Each year has a DEFAULT state partition to catch all states automatically.
--   Specific state sub-partitions can be added later without data migration.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS credit_risk;

-- Separate sequence for id (BIGSERIAL cannot be used on partitioned parent table)
CREATE SEQUENCE IF NOT EXISTS credit_risk.stg_loans_id_seq;

-- =============================================================================
-- STAGING LAYER — Parent Table (no data stored here, routing only)
-- =============================================================================
CREATE TABLE IF NOT EXISTS credit_risk.stg_loans (
    id                      BIGINT DEFAULT nextval('credit_risk.stg_loans_id_seq'),

    -- Identification
    activity_year           INT,
    lei                     VARCHAR(20),
    state_code              VARCHAR(2)  NOT NULL,  -- Level-2 partition key
    county_code             VARCHAR(5),
    census_tract            VARCHAR(11),

    -- Loan Details
    loan_type               SMALLINT,
    loan_purpose            SMALLINT,
    loan_amount             DOUBLE PRECISION,
    interest_rate           DOUBLE PRECISION,
    property_value          DOUBLE PRECISION,
    occupancy_type          SMALLINT,
    lien_status             SMALLINT,

    -- Borrower Financials
    income                  INT,
    applicant_age           VARCHAR(10),

    -- Computed Risk Metrics (from Spark enrichment)
    loan_to_value_ratio     DOUBLE PRECISION,
    debt_to_income_numeric  DOUBLE PRECISION,
    mkt_benchmark           DOUBLE PRECISION,       -- Avg annual FRED rate
    interest_rate_spread    DOUBLE PRECISION,       -- interest_rate - mkt_benchmark

    -- Metadata
    data_year               INT NOT NULL,           -- Level-1 partition key
    loaded_at               TIMESTAMP DEFAULT NOW()

) PARTITION BY LIST (data_year);


-- =============================================================================
-- INDEXES (defined on parent — PostgreSQL auto-applies to all partitions)
-- =============================================================================
CREATE INDEX IF NOT EXISTS idx_stg_loans_ltv    ON credit_risk.stg_loans (loan_to_value_ratio);
CREATE INDEX IF NOT EXISTS idx_stg_loans_spread ON credit_risk.stg_loans (interest_rate_spread);
CREATE INDEX IF NOT EXISTS idx_stg_loans_lei    ON credit_risk.stg_loans (lei);


-- =============================================================================
-- MART LAYER (placeholder — will be populated by dbt in Phase 4.2)
-- =============================================================================
-- CREATE TABLE IF NOT EXISTS credit_risk.fct_credit_risk (...);
