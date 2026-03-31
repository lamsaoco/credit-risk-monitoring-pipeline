
-- Mart layer: Productive loan risk fact table enriched with lookup descriptions
with stg_loans as (
    select * from {{ ref('stg_loans') }}
),

-- Lookups from seeds (mapped to correct column names)
loan_types as ( select * from {{ ref('loan_type') }} ),
loan_purposes as ( select * from {{ ref('loan_purpose') }} ),
occupancy_types as ( select * from {{ ref('occupancy_type') }} ),
lien_statuses as ( select * from {{ ref('lien_status') }} ),
states as ( select * from {{ ref('state_mapping') }} )

select
    -- Identifiers
    l.id,
    l.lei,
    l.state_code,
    s.state_name,
    l.county_code,
    l.census_tract,
    l.data_year,

    -- Financial Data
    l.loan_amount,
    l.property_value,
    l.income,
    l.interest_rate,

    -- Descriptive Names from Seeds (using 'description' and 'state_name' columns)
    lt.description as loan_type_name,
    lp.description as loan_purpose_name,
    ot.description as occupancy_type_name,
    ls.description as lien_status_name,

    -- Risk Indicators
    l.loan_to_value_ratio,
    l.debt_to_income_numeric,
    l.mkt_benchmark,
    l.interest_rate_spread,

    -- Business Logic: Risk Segmentation
    case
        when l.loan_to_value_ratio > 0.8 or l.debt_to_income_numeric > 43 then 'High'
        when l.loan_to_value_ratio between 0.6 and 0.8 and l.debt_to_income_numeric <= 43 then 'Medium'
        else 'Low'
    end as risk_segment,

    -- Business Logic: Cost Category
    case
        when l.interest_rate_spread > 0 then 'High Cost'
        else 'Market/Subsidized'
    end as cost_category,

    current_timestamp as mart_updated_at

from stg_loans l
left join loan_types lt on l.loan_type = lt.id
left join loan_purposes lp on l.loan_purpose = lp.id
left join occupancy_types ot on l.occupancy_type = ot.id
left join lien_statuses ls on l.lien_status = ls.id
left join states s on l.state_code = s.state_code
