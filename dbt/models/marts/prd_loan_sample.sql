SELECT 
    data_year,
    state_code,
    state_name,
    income,
    interest_rate_spread,
    risk_segment,
    loan_amount,
    loan_purpose_name
FROM {{ source('credit_risk_prod', 'fct_loan_risk') }}
ORDER BY RANDOM()
LIMIT 10000