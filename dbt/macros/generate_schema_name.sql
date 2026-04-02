-- =============================================================================
-- Custom schema name resolver — supports PostgreSQL and Snowflake simultaneously.
--
-- PostgreSQL behavior (target.type = 'postgres'):
--   default dbt behavior: profile_schema + "_" + custom_schema
--   e.g. staging models  → credit_risk_stg
--        marts models     → credit_risk_prod
--
-- Snowflake behavior (target.type = 'snowflake'):
--   use custom_schema DIRECTLY (uppercase), bypassing the default concat logic.
--   e.g. +schema: stg  → STG   (matches CREDIT_RISK_DW.STG  in schema.sql)
--        +schema: prod → PROD  (matches CREDIT_RISK_DW.PROD in schema.sql)
--
-- Result: dbt_project.yml schema names (stg / prod) work correctly on BOTH
--         databases without any per-run configuration change.
-- =============================================================================

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if target.type == 'snowflake' -%}
        {# Snowflake: use the custom schema directly in uppercase #}
        {%- if custom_schema_name is not none -%}
            {{ custom_schema_name | upper | trim }}
        {%- else -%}
            {{ target.schema | upper | trim }}
        {%- endif -%}

    {%- else -%}
        {# PostgreSQL (and others): standard dbt behavior — prefix with base schema #}
        {%- if custom_schema_name is not none -%}
            {{ target.schema | trim }}_{{ custom_schema_name | trim }}
        {%- else -%}
            {{ target.schema | trim }}
        {%- endif -%}

    {%- endif -%}

{%- endmacro %}
