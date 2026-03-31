
-- Staging layer: Thin cleaning and renaming for the Marts layer
with source as (
    select * from {{ source('credit_risk_source', 'stg_loans') }}
)

select
    -- IDs & Metadata
    id,
    lei,
    state_code,
    county_code,
    census_tract,
    data_year,
    activity_year,

    -- Financials
    loan_type,
    loan_purpose,
    loan_amount,
    interest_rate,
    property_value,
    income,
    occupancy_type,
    lien_status,

    -- Risk Metrics (pre-calculated in Spark)
    loan_to_value_ratio,
    debt_to_income_numeric,
    mkt_benchmark,
    interest_rate_spread,

    current_timestamp as dbt_updated_at
from source
