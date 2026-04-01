{{ config(materialized='table') }}

SELECT 
    data_year,
    state_code,
    state_name,
    income,
    interest_rate_spread,
    risk_segment,
    loan_amount,
    loan_purpose_name
FROM {{ ref('fct_loan_risk') }}
-- Shuffle the data to ensure the sample is representative of the whole population
ORDER BY RANDOM()
-- Limit to 10k rows to optimize browser rendering performance
LIMIT 10000