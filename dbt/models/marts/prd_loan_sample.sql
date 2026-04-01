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
-- Remove missing/invalid data and massive outliers (income > 1000k)
WHERE income > 0 AND income < 1000 
  AND interest_rate_spread IS NOT NULL 
  AND interest_rate_spread != 0
-- Shuffle the data to ensure the sample is representative of the whole population
ORDER BY RANDOM()
-- Limit to 10k rows to optimize browser rendering performance
LIMIT 10000