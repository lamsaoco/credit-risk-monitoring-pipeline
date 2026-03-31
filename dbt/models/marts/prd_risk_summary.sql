
WITH base_data AS (
    SELECT * FROM {{ source('credit_risk_prod', 'fct_loan_risk') }}
)

SELECT 
    data_year,
    state_code,
    state_name,
    risk_segment,
    loan_purpose_name,
    -- Các chỉ số cơ bản
    COUNT(*) as total_loans,
    SUM(loan_amount) as total_loan_amount,
    AVG(interest_rate) as avg_interest_rate,
    AVG(interest_rate_spread) as avg_rate_spread,
    -- Chỉ số rủi ro nâng cao (Bad Loan Ratio cho Heatmap)
    COUNT(*) FILTER (WHERE risk_segment = 'High')::FLOAT / COUNT(*) as high_risk_ratio
FROM base_data
GROUP BY 1, 2, 3, 4, 5