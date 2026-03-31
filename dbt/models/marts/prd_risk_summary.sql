WITH base_data AS (
    SELECT * FROM {{ ref('fct_loan_risk') }}
)

SELECT 
    data_year,
    state_code,
    state_name,
    risk_segment,
    loan_purpose_name,
    -- Đổi tên thành loan_count để khớp với app.py
    COUNT(*) as loan_count, 
    SUM(loan_amount) as total_loan_amount,
    -- Thêm SUM(interest_rate) để App tính lại Weighted Average khi Filter
    SUM(interest_rate) as sum_interest_rate, 
    AVG(interest_rate_spread) as avg_rate_spread,
    -- Cột này dùng cho Heatmap
    COUNT(*) FILTER (WHERE risk_segment = 'High')::FLOAT / COUNT(*) as high_risk_ratio
FROM base_data
GROUP BY 1, 2, 3, 4, 5