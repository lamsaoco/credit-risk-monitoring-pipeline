{{ config(materialized='table') }}

WITH base_data AS (
    -- Import the core fact table from the staging/intermediate layer
    SELECT * FROM {{ ref('fct_loan_risk') }}
)

SELECT 
    data_year,
    state_code,
    state_name,
    risk_segment,
    loan_purpose_name,
    -- Aggregated metrics for high-level dashboarding
    COUNT(*) as loan_count,
    SUM(loan_amount) as total_loan_amount,
    -- Using SUM instead of AVG for accurate weighted calculations in the frontend
    SUM(interest_rate) as sum_interest_rate, 
    AVG(interest_rate_spread) as avg_rate_spread,
    -- Pre-calculating risk ratio for geographical heatmaps
    -- Cross-database compatible: FILTER clause is PostgreSQL-only; CASE WHEN works on both PG and Snowflake
    COUNT(CASE WHEN risk_segment = 'High' THEN 1 END) * 1.0 / NULLIF(COUNT(*), 0) as high_risk_ratio
FROM base_data
GROUP BY 1, 2, 3, 4, 5