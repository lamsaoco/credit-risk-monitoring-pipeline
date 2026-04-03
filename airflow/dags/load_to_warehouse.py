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
from airflow.operators.python import PythonOperator, ShortCircuitOperator
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
    Returns False (triggering ShortCircuit halt) if staging data is missing.
    Returns True to allow downstream tasks to proceed.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    year = kwargs.get("params", {}).get("year") or kwargs.get("logical_date").year
    bucket = Variable.get("s3_bucket_name")
    prefix = f"{S3_STAGING_PREFIX}/{year}/"

    hook = S3Hook(aws_conn_id="aws_default")
    if not hook.list_keys(bucket, prefix=prefix, max_items=1):
        print(f"❌ Validation FAILED: Staging data for year {year} NOT found at s3://{bucket}/{prefix}.")
        print("Halting DAG — no downstream tasks will run.")
        return False

    print(f"✅ Staging landing validated for year {year}. Proceeding...")
    return True


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


# Columns shared by both backends
_TARGET_COLUMNS = [
    "activity_year", "lei", "state_code", "county_code", "census_tract",
    "loan_type", "loan_purpose", "loan_amount", "interest_rate",
    "property_value", "occupancy_type", "lien_status",
    "income", "applicant_age",
    "loan_to_value_ratio", "debt_to_income_numeric",
    "mkt_benchmark", "interest_rate_spread",
    "data_year",
]

# Batch size for PostgreSQL — moderate size to keep RAM stable (~300MB/batch)
_PG_BATCH_SIZE = 500_000

# Batch size for Snowflake — larger batches = fewer round trips = faster bulk load
# 2M rows × ~20 columns × ~8 bytes ≈ 320MB per batch, safe on 8GB RAM
_SF_BATCH_SIZE = 2_000_000


# ─────────────────────────────────────── #
#    TASK 3 — Dual Backend Load Logic     #
# ─────────────────────────────────────── #
def load_to_warehouse(**kwargs):
    """
    Streams staging Parquet into the configured DW backend using PyArrow
    iter_batches(). RAM usage stays at ~1 chunk (~500k rows) at any time,
    regardless of total dataset size. Fixes OOM on 8 GB RAM EC2 instances.
    """
    import pyarrow.dataset as pad

    year = kwargs.get("params", {}).get("year") or kwargs.get("logical_date").year
    workspace = get_workspace(year)
    local_parquet_dir = os.path.join(workspace, f"{S3_STAGING_PREFIX}/{year}")

    if not os.path.exists(local_parquet_dir):
        raise FileNotFoundError(f"Staging directory not found: {local_parquet_dir}")

    if DW_BACKEND == "postgresql":
        _BATCH_SIZE = _PG_BATCH_SIZE
    elif DW_BACKEND == "snowflake":
       _BATCH_SIZE = _SF_BATCH_SIZE

    print(f"📦 Streaming {local_parquet_dir} → {DW_BACKEND.upper()} (batch={_BATCH_SIZE:,} rows)")

    # pyarrow.dataset with partitioning="hive" reconstructs partition columns (e.g. state_code)
    # from folder names like state_code=CA/, state_code=TX/ — same behavior as pd.read_parquet()
    dataset = pad.dataset(local_parquet_dir, format="parquet", partitioning="hive")

    if DW_BACKEND == "postgresql":
        _stream_to_postgres(dataset, year)
    elif DW_BACKEND == "snowflake":
        _stream_to_snowflake(dataset, year)
    else:
        raise ValueError(f"Unknown DW_BACKEND: '{DW_BACKEND}'. Must be 'postgresql' or 'snowflake'.")


# ──────────────────────────────── #
#  PostgreSQL streaming loader     #
# ──────────────────────────────── #
def _stream_to_postgres(dataset, year: int):
    """Stream PyArrow dataset into PostgreSQL one batch at a time."""
    import psycopg2
    from psycopg2.extras import execute_values

    conn = psycopg2.connect(
        host="postgres-dw",
        port=5432,
        dbname=os.getenv("DW_POSTGRES_DB", "credit_risk_dw"),
        user=os.getenv("DW_POSTGRES_USER"),
        password=os.getenv("DW_POSTGRES_PASSWORD"),
    )

    total_rows = 0
    seen_states = set()

    try:
        with conn:
            with conn.cursor() as cur:
                # ── Step 1: Idempotent cleanup (runs ONCE before any batch) ──
                cur.execute("DELETE FROM credit_risk.stg_loans WHERE data_year = %s", (year,))
                print(f"   🗑️  Deleted {cur.rowcount:,} existing rows for year {year}.")

                # ── Step 2: Create year-level and default partitions (ONCE) ──
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS credit_risk.stg_loans_y{year}
                    PARTITION OF credit_risk.stg_loans FOR VALUES IN ({year})
                    PARTITION BY LIST (state_code);
                """)
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS credit_risk.stg_loans_y{year}_default
                    PARTITION OF credit_risk.stg_loans_y{year} DEFAULT;
                """)

                # ── Step 3: Stream each batch ──
                for batch in dataset.to_batches(batch_size=_PG_BATCH_SIZE):
                    df_chunk = batch.to_pandas()
                    df_chunk["data_year"] = year

                    # Retain only target columns present in this chunk
                    available = [c for c in _TARGET_COLUMNS if c in df_chunk.columns]
                    df_chunk = df_chunk[available]

                    # Type coercion
                    int_cols = ["loan_type", "loan_purpose", "occupancy_type",
                                "lien_status", "data_year", "activity_year"]
                    for col in int_cols:
                        if col in df_chunk.columns:
                            df_chunk[col] = df_chunk[col].fillna(0).astype(int)
                    if "income" in df_chunk.columns:
                        df_chunk["income"] = df_chunk["income"].fillna(0).astype(float).astype(int)

                    # Create state sub-partitions for NEW states discovered in this batch
                    batch_states = set(df_chunk["state_code"].dropna().unique())
                    new_states = batch_states - seen_states
                    for state in new_states:
                        safe_state = "".join(e for e in str(state) if e.isalnum())
                        val_state  = str(state).replace("'", "''")
                        if safe_state:
                            cur.execute(
                                f"CREATE TABLE IF NOT EXISTS credit_risk.stg_loans_y{year}_{safe_state} "
                                f"PARTITION OF credit_risk.stg_loans_y{year} FOR VALUES IN ('{val_state}');"
                            )
                    seen_states |= new_states

                    # Bulk insert
                    cols = ", ".join(available)
                    execute_values(
                        cur,
                        f"INSERT INTO credit_risk.stg_loans ({cols}) VALUES %s",
                        df_chunk.itertuples(index=False, name=None),
                        page_size=10_000,
                    )

                    total_rows += len(df_chunk)
                    print(f"   ➡️  Inserted batch — running total: {total_rows:,} rows")

                    # Explicitly free batch memory before next iteration
                    del df_chunk

    finally:
        conn.close()

    print(f"✅ PostgreSQL load complete: {total_rows:,} rows for year {year}.")


# ──────────────────────────────── #
#  Snowflake streaming loader      #
# ──────────────────────────────── #
def _stream_to_snowflake(dataset, year: int):
    """Stream PyArrow dataset into Snowflake one batch at a time."""
    from airflow.hooks.base import BaseHook
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas

    conn_info = BaseHook.get_connection("snowflake_default")
    extra     = conn_info.extra_dejson

    sf_conn = snowflake.connector.connect(
        account   = extra.get("account"),
        user      = conn_info.login,
        password  = conn_info.password,
        database  = extra.get("database", "CREDIT_RISK_DW"),
        schema    = extra.get("schema",   "RAW"),
        warehouse = extra.get("warehouse", "COMPUTE_WH"),
        role      = extra.get("role",     "SYSADMIN"),
    )

    total_rows = 0

    try:
        # ── Step 1: Idempotent cleanup (runs ONCE before any batch) ──
        with sf_conn.cursor() as cur:
            cur.execute("DELETE FROM STG_LOANS WHERE DATA_YEAR = %s", (year,))
            print(f"   🗑️  Deleted {cur.rowcount:,} existing rows for year {year}.")

        # ── Step 2: Stream each batch, accumulating to SF_BATCH_SIZE before uploading ──
        # PyArrow to_batches() returns small chunks limited by Parquet row groups.
        # We accumulate them until reaching _SF_BATCH_SIZE for efficient Snowflake bulk load.
        import pyarrow as pa

        pending_batches = []
        pending_rows = 0

        def _flush_to_snowflake(batches: list):
            """Concatenate accumulated Arrow batches and upload as one DataFrame."""
            combined = pa.concat_batches(batches).to_pandas()
            combined["data_year"] = year

            available = [c for c in _TARGET_COLUMNS if c in combined.columns]
            combined = combined[available].copy()
            combined.columns = [c.upper() for c in combined.columns]
            combined["DATA_YEAR"] = year

            int_cols = ["LOAN_TYPE", "LOAN_PURPOSE", "OCCUPANCY_TYPE",
                        "LIEN_STATUS", "DATA_YEAR", "ACTIVITY_YEAR"]
            for col in int_cols:
                if col in combined.columns:
                    combined[col] = combined[col].fillna(0).astype(int)

            success, _, nrows, _ = write_pandas(
                conn=sf_conn, df=combined,
                table_name="STG_LOANS", schema="RAW",
                auto_create_table=False,
            )
            if not success:
                raise RuntimeError("write_pandas reported failure for this batch.")
            return len(combined), nrows

        for batch in dataset.to_batches(batch_size=_SF_BATCH_SIZE):
            pending_batches.append(batch)
            pending_rows += batch.num_rows

            if pending_rows >= _SF_BATCH_SIZE:
                n_uploaded, nrows = _flush_to_snowflake(pending_batches)
                total_rows += n_uploaded
                print(f"   ➡️  Uploaded {nrows:,} rows — running total: {total_rows:,}")
                pending_batches = []
                pending_rows = 0
                del n_uploaded

        # Flush any remaining rows that didn't fill a full batch
        if pending_batches:
            n_uploaded, nrows = _flush_to_snowflake(pending_batches)
            total_rows += n_uploaded
            print(f"   ➡️  Uploaded final batch ({nrows:,} rows) — running total: {total_rows:,}")

    finally:
        sf_conn.close()

    print(f"✅ Snowflake load complete: {total_rows:,} rows for year {year}.")


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

    t1 = ShortCircuitOperator(
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
