"""
DAG #3: load_to_warehouse
=========================
Reads enriched Parquet files from S3 staging/ and loads into the
configured Data Warehouse (PostgreSQL or Snowflake).

Toggle between backends by setting DW_BACKEND in .env:
  DW_BACKEND=postgresql  → loads to credit_risk_dw (local Docker postgres)
  DW_BACKEND=snowflake   → loads to CREDIT_RISK_DW (Snowflake cloud)

Task Flow:
  validate_staging_landing
        ↓
  download_staging_to_local  (boto3 download)
        ↓
  load_to_warehouse          (pandas → DW)
        ↓
  cleanup_local              (trigger_rule=all_done)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import os
import shutil

# ─────────────────────────────────────────── #
# Backend switch (set via DW_BACKEND env var) #
# ─────────────────────────────────────────── #
DW_BACKEND = os.getenv("DW_BACKEND", "postgresql")

STAGING_WORKSPACE_PREFIX = "/tmp/mortgage_load_workspace_"
S3_STAGING_PREFIX = "staging/mortgage_curated"


def get_workspace(year: int) -> str:
    return f"{STAGING_WORKSPACE_PREFIX}{year}"


# ─────────────────── #
# Failure Notification #
# ─────────────────── #
def notify_failure(context):
    ti = context.get("task_instance")
    exception = context.get("exception")
    receiver = Variable.get("email_receiver")
    subject = f"⚠️ [INCIDENT] Warehouse Load Failed: {ti.task_id}"
    html_content = f"""
    <h3>Credit Risk DW — Load Pipeline Failure</h3>
    <p><b>DAG:</b> {ti.dag_id} | <b>Task:</b> {ti.task_id}</p>
    <p><b>Backend:</b> {DW_BACKEND}</p>
    <p style="color:darkred; border-left:4px solid red; padding-left:10px;">
        <b>Error:</b><br/>{str(exception)}
    </p>
    <p><a href="{ti.log_url}">View Full Logs</a></p>
    """
    send_email(to=receiver, subject=subject, html_content=html_content)


default_args = {
    "owner": "PhanNH",
    "start_date": datetime(2026, 3, 26),
    "retries": 0,
    "on_failure_callback": notify_failure,
}

# ─────────────── #
#    TASK 1       #
# ─────────────── #
def validate_staging_landing(**kwargs):
    """
    Guard check: ensure enriched Parquet for the target year exists on S3.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    year = kwargs.get("params", {}).get("year") or kwargs.get("logical_date").year
    bucket = Variable.get("s3_bucket_name")
    prefix = f"{S3_STAGING_PREFIX}/{year}/"

    hook = S3Hook(aws_conn_id="aws_default")
    if not hook.list_keys(bucket, prefix=prefix, max_items=1):
        raise FileNotFoundError(
            f"Staging data for year {year} not found at s3://{bucket}/{prefix}. "
            f"Run the 'mortgage_risk_silver_enrichment' DAG first."
        )
    print(f"✅ Staging landing validated for year {year}.")


# ─────────────── #
#    TASK 2       #
# ─────────────── #
def download_staging_to_local(**kwargs):
    """
    Downloads enriched Parquet files from S3 staging/ to local disk.
    Bypasses s3a:// protocol — uses plain boto3 (no hadoop-aws dependency).
    """
    import boto3

    year = kwargs.get("params", {}).get("year") or kwargs.get("logical_date").year
    bucket = Variable.get("s3_bucket_name")
    workspace = get_workspace(year)
    prefix = f"{S3_STAGING_PREFIX}/{year}/"

    os.makedirs(workspace, exist_ok=True)
    s3 = boto3.client("s3")

    paginator = s3.get_paginator("list_objects_v2")
    downloaded = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            local_path = os.path.join(workspace, key)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            s3.download_file(bucket, key, local_path)
            downloaded += 1

    print(f"✅ Downloaded {downloaded} Parquet files to {workspace}")


# ─────────────────────────────────────── #
#    TASK 3 — Dual Backend Load Logic     #
# ─────────────────────────────────────── #
def load_to_warehouse(**kwargs):
    """
    Reads staging Parquet and loads into the configured DW backend.
    Controlled by the DW_BACKEND environment variable.
    """
    import pandas as pd

    year = kwargs.get("params", {}).get("year") or kwargs.get("logical_date").year
    workspace = get_workspace(year)
    local_parquet_dir = os.path.join(workspace, f"{S3_STAGING_PREFIX}/{year}")

    print(f"📦 Loading data for year {year} → Backend: {DW_BACKEND.upper()}")

    # ── Read all Parquet partitions into one DataFrame ──
    df = pd.read_parquet(local_parquet_dir)
    df["data_year"] = year
    record_count = len(df)
    print(f"   Read {record_count:,} rows from local Parquet.")

    if DW_BACKEND == "postgresql":
        _load_postgres(df, year)
    elif DW_BACKEND == "snowflake":
        _load_snowflake(df, year)
    else:
        raise ValueError(f"Unknown DW_BACKEND: '{DW_BACKEND}'. Must be 'postgresql' or 'snowflake'.")

    print(f"✅ Loaded {record_count:,} rows to {DW_BACKEND.upper()} for year {year}.")


def _load_postgres(df, year: int):
    """Load DataFrame into PostgreSQL credit_risk.stg_loans."""
    import os
    import psycopg2
    from psycopg2.extras import execute_values

    conn = psycopg2.connect(
        host="postgres-dw",
        port=5432,
        dbname=os.getenv("DW_POSTGRES_DB", "credit_risk_dw"),
        user=os.getenv("DW_POSTGRES_USER"),
        password=os.getenv("DW_POSTGRES_PASSWORD"),
    )

    # Columns that exist in both parquet and the target table
    target_columns = [
        "activity_year", "lei", "state_code", "county_code", "census_tract",
        "loan_type", "loan_purpose", "loan_amount", "interest_rate",
        "property_value", "occupancy_type", "lien_status",
        "income", "applicant_age",
        "loan_to_value_ratio", "debt_to_income_numeric",
        "mkt_benchmark", "interest_rate_spread",
        "data_year",
    ]

    # Filter to only available columns (defensive)
    available = [c for c in target_columns if c in df.columns]
    df_insert = df[available].copy()

    # Pre-processing: Ensure NaNs in integer columns are handled and types match Postgres
    # (Pandas often defaults to float64 for columns with NaNs, which can cause issues)
    int_cols = ["loan_type", "loan_purpose", "occupancy_type", "lien_status", "data_year", "activity_year"]
    for col in int_cols:
        if col in df_insert.columns:
            df_insert[col] = df_insert[col].fillna(0).astype(int)

    if "income" in df_insert.columns:
        df_insert["income"] = df_insert["income"].fillna(0).astype(float).astype(int)

    with conn:
        with conn.cursor() as cur:
            # 1. Idempotent: delete existing rows for this year before re-inserting
            # This empties any existing default partition, allowing new state partitions to be added safely.
            cur.execute(
                "DELETE FROM credit_risk.stg_loans WHERE data_year = %s", (year,)
            )
            deleted = cur.rowcount
            print(f"   Deleted {deleted} existing rows for year {year}.")

            # 2. Dynamic Partitioning: Auto-create Year level partition
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS credit_risk.stg_loans_y{year}
                PARTITION OF credit_risk.stg_loans FOR VALUES IN ({year})
                PARTITION BY LIST (state_code);
            """)

            # 3. Dynamic Partitioning: Auto-create State level sub-partitions
            unique_states = df["state_code"].dropna().unique()
            for state in unique_states:
                safe_state = "".join(e for e in str(state) if e.isalnum())
                if safe_state:
                    # Escape single quotes in state names just in case
                    val_state = str(state).replace("'", "''")
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS credit_risk.stg_loans_y{year}_{safe_state}
                        PARTITION OF credit_risk.stg_loans_y{year} FOR VALUES IN ('{val_state}');
                    """)

            # 4. Dynamic Partitioning: Auto-create DEFAULT state partition
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS credit_risk.stg_loans_y{year}_default
                PARTITION OF credit_risk.stg_loans_y{year} DEFAULT;
            """)

            # 5. Execute Values with Progress Tracking
            total_rows = len(df_insert)
            chunk_size = 100000  # Print progress every 100k rows
            cols = ", ".join(available)
            insert_query = f"INSERT INTO credit_risk.stg_loans ({cols}) VALUES %s"

            print(f"   🚀 Starting insert of {total_rows:,} rows...")

            for i in range(0, total_rows, chunk_size):
                chunk = df_insert.iloc[i : i + chunk_size]
                execute_values(
                    cur,
                    insert_query,
                    chunk.itertuples(index=False, name=None),
                    page_size=10000,
                )
                
                rows_done = min(i + chunk_size, total_rows)
                percent = (rows_done / total_rows) * 100
                print(f"   ➡️ Progress: {percent:6.1f}% | Processed {rows_done:9,} / {total_rows:,} rows")

    conn.close()


def _load_snowflake(df, year: int):
    """Load DataFrame into Snowflake STG.STG_LOANS.
    Idempotent: deletes existing rows for the year before re-inserting,
    matching the behaviour of the PostgreSQL loader for consistency.
    """
    from airflow.hooks.base import BaseHook
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas

    print(f"   ❄️ Connecting to Snowflake...")
    conn_info = BaseHook.get_connection("snowflake_default")

    extra = conn_info.extra_dejson
    sf_conn = snowflake.connector.connect(
        account=extra.get("account"),
        user=conn_info.login,
        password=conn_info.password,
        database=extra.get("database", "CREDIT_RISK_DW"),
        schema=extra.get("schema", "RAW"),   # RAW matches terraform schema raw
        warehouse=extra.get("warehouse", "COMPUTE_WH"),
        role=extra.get("role", "SYSADMIN"),
    )

    # Columns that exist in the target table (match with Postgres logic)
    target_columns = [
        "activity_year", "lei", "state_code", "county_code", "census_tract",
        "loan_type", "loan_purpose", "loan_amount", "interest_rate",
        "property_value", "occupancy_type", "lien_status",
        "income", "applicant_age",
        "loan_to_value_ratio", "debt_to_income_numeric",
        "mkt_benchmark", "interest_rate_spread",
        "data_year",
    ]

    # Filter to only available columns (defensive)
    available = [c for c in target_columns if c in df.columns]
    df_sf = df[available].copy()
    
    # Uppercase columns for Snowflake compatibility
    df_sf.columns = [c.upper() for c in df_sf.columns]
    df_sf["DATA_YEAR"] = year

    # Ensure integer columns don't have NaNs causing float issues
    int_cols = ["LOAN_TYPE", "LOAN_PURPOSE", "OCCUPANCY_TYPE", "LIEN_STATUS", "DATA_YEAR", "ACTIVITY_YEAR"]
    for col in int_cols:
        if col in df_sf.columns:
            df_sf[col] = df_sf[col].fillna(0).astype(int)

    # Idempotent: delete existing rows for this year before re-inserting
    # Mirrors the PostgreSQL DELETE logic so re-runs are always safe.
    with sf_conn.cursor() as cur:
        cur.execute("DELETE FROM STG_LOANS WHERE DATA_YEAR = %s", (year,))
        deleted = cur.rowcount
        print(f"   Deleted {deleted} existing rows for year {year}.")

    total_rows = len(df_sf)
    chunk_size = 500000  # Larger chunks for Snowflake (efficient bulk load)

    print(f"   🚀 Starting bulk upload to Snowflake ({total_rows:,} rows)...")

    for i in range(0, total_rows, chunk_size):
        chunk = df_sf.iloc[i : i + chunk_size]
        success, nchunks, nrows, _ = write_pandas(
            conn=sf_conn,
            df=chunk,
            table_name="STG_LOANS",
            schema="RAW",             # Explicit schema — avoids relying solely on connection default
            auto_create_table=False,
        )

        if not success:
            raise RuntimeError(f"Snowflake load failed at row {i}")

        rows_done = min(i + chunk_size, total_rows)
        percent = (rows_done / total_rows) * 100
        print(f"   ➡️ Progress: {percent:6.1f}% | Uploaded {rows_done:9,} / {total_rows:,} rows")

    sf_conn.close()


# ─────────────── #
#    TASK 4       #
# ─────────────── #
def cleanup_local(**kwargs):
    """Remove local workspace regardless of upstream task status."""
    year = kwargs.get("params", {}).get("year") or kwargs.get("logical_date").year
    workspace = get_workspace(year)
    if os.path.exists(workspace):
        shutil.rmtree(workspace)
        print(f"✅ Cleaned up local workspace: {workspace}")


# ─────────────────────── #
#    DAG Orchestration    #
# ─────────────────────── #
with DAG(
    "credit_risk_load_warehouse",
    default_args=default_args,
    description=f"Load enriched mortgage data from S3 staging → {DW_BACKEND.upper()} DW",
    schedule_interval=None, # Triggered exclusively by the upstream Transform DAG
    catchup=False,
    render_template_as_native_obj=True,
    tags=["warehouse", "load", DW_BACKEND],
    params={
        "year": Param(2024, type="integer", title="Processing Year"),
    },
) as dag:

    t1 = PythonOperator(
        task_id="validate_staging_landing",
        python_callable=validate_staging_landing,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="download_staging_to_local",
        python_callable=download_staging_to_local,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_to_warehouse,
        provide_context=True,
    )

    t4 = PythonOperator(
        task_id="cleanup_local",
        python_callable=cleanup_local,
        provide_context=True,
        trigger_rule="all_done",
    )

    t5 = TriggerDagRunOperator(
        task_id="trigger_dbt_marts",
        trigger_dag_id="credit_risk_dbt_marts",
        conf={"year": "{{ params.year }}"},
        wait_for_completion=False, # Fire-and-forget: offload execution to the dbt DAG without blocking here
    )

    t1 >> t2 >> t3 >> t4 >> t5
