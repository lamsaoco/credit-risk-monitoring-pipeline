from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import os
import sys
import shutil

# --- Constants & Schema Definition ---
# Explicit schema avoids 'inferSchema' overhead, crucial for 12M+ records
LOCAL_TMP_DIR = "/tmp/hmda_production_workspace"

default_args = {
    'owner': 'PhanNH',
    'start_date': datetime(2026, 3, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_spark():
    """
    Initialize Spark session optimized for m7i-flex.large (8GB RAM).
    Spark is used only for local CSV reading and processing — S3 upload is
    handled separately via boto3, bypassing hadoop-aws entirely.
    """
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName("Credit_Risk_Full_Ingestion") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.sql.shuffle.partitions", "50") \
        .config("spark.sql.ansi.enabled", "false") \
        .getOrCreate()

def send_status_email(status, task_name, year, details=""):
    """
    Sends operational alerts via Airflow's SMTP configuration.
    """
    try:
        receiver = Variable.get("email_receiver")
        subject = f"[Airflow Alert] {task_name} {status} - Year {year}"
        html_content = f"""
        <h3>Pipeline Execution Report</h3>
        <p><b>Task:</b> {task_name}</p>
        <p><b>Status:</b> {status}</p>
        <p><b>Target Year:</b> {year}</p>
        <p><b>Details:</b> {details}</p>
        <p><b>Timestamp:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        """
        send_email(to=receiver, subject=subject, html_content=html_content)
    except Exception as e:
        print(f"Failed to send email: {str(e)}")

# --- FRED API Task ---
def ingest_fred_to_raw():
    """
    Ingests interest rate data from FRED API.
    Uses local imports to prevent Airflow DAG parsing timeouts.
    """
    from fredapi import Fred
    import pandas as pd
    
    api_key = Variable.get("fred_api_key")
    s3_bucket = Variable.get("s3_bucket_name")
    
    try:
        fred = Fred(api_key=api_key)
        series_data = fred.get_series('FEDFUNDS')
        df_pd = pd.DataFrame(series_data, columns=['fed_rate']).reset_index()
        df_pd.rename(columns={'index': 'observation_date'}, inplace=True)
        
        # Save to S3 Raw folder
        output_s3 = f"s3://{s3_bucket}/raw/fred_raw/data.parquet"
        df_pd.to_parquet(output_s3)
        print(f"✅ FRED data successfully updated in S3 Raw.")
    except Exception as e:
        print(f"❌ FRED Ingestion Error: {str(e)}")
        raise e

# --- HMDA Modular Tasks ---

def get_hmda_workspace(year):
    return f"/tmp/hmda_workspace_{year}"

def download_hmda_data(**kwargs):
    """
    Task 1: Download and extract the HMDA LAR ZIP file.
    Returns the extracted CSV filename via XCom.
    """
    import os
    import requests
    import zipfile
    
    conf = kwargs.get('dag_run').conf or {}
    year = conf.get('year', 2024)
    workspace = get_hmda_workspace(year)
    os.makedirs(workspace, exist_ok=True)
    
    download_url = f"https://files.ffiec.cfpb.gov/dynamic-data/{year}/{year}_lar.zip"
    zip_path = os.path.join(workspace, f"hmda_{year}.zip")
    
    print(f"Downloading HMDA {year} dataset...")
    with requests.get(download_url, stream=True) as r:
        r.raise_for_status()
        with open(zip_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024*1024):
                f.write(chunk)
    
    print("Extracting ZIP...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(workspace)
        extracted_csv = zip_ref.namelist()[0]
    
    csv_path = os.path.join(workspace, extracted_csv)
    print(f"Extracted CSV to {csv_path}")
    
    # Pass the filename down to the next task
    return extracted_csv

def process_hmda_spark(**kwargs):
    """
    Task 2: PySpark ETL. Reads the downloaded CSV, filters, casts to Parquet,
    and partitions the local output.
    """
    import os
    import sys
    from pyspark.sql.types import ShortType, IntegerType, DoubleType, StringType
    from pyspark.sql import functions as F
    
    # Ensure Spark uses the correct Python interpreter from .venv
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    conf = kwargs.get('dag_run').conf or {}
    year = conf.get('year', 2024)
    workspace = get_hmda_workspace(year)
    
    # Pull the CSV filename from the download task
    ti = kwargs['ti']
    extracted_csv = ti.xcom_pull(task_ids='download_hmda_data')
    if not extracted_csv:
        raise ValueError("No extracted CSV returned from download_hmda_data task!")
        
    local_file = os.path.join(workspace, extracted_csv)
    
    # Schema: https://ffiec.cfpb.gov/documentation/publications/loan-level-datasets/public-lar-schema
    TARGET_SCHEMA = {
        # Identification & Geography
        "activity_year":             IntegerType(),   # 4-digit year
        "lei":                       StringType(),    # 20-char LEI identifier
        "state_code":                StringType(),    # 2-char FIPS state (e.g. "CA")
        "county_code":               StringType(),    # 5-char FIPS county
        "census_tract":              StringType(),    # 11-char census tract
        # Loan Details
        "loan_type":                 ShortType(),     # 1=Conventional, 2=FHA, 3=VA, 4=USDA
        "loan_purpose":              ShortType(),     # 1=Purchase, 2=Improvement, 31=Refi…
        "loan_amount":               DoubleType(),    # In dollars
        "interest_rate":             DoubleType(),    # Percent; "NA"/"Exempt" → NULL
        "property_value":            DoubleType(),    # In dollars; "NA"/"Exempt" → NULL
        "occupancy_type":            ShortType(),     # 1=Principal, 2=Second, 3=Investment
        "total_loan_costs":          DoubleType(),    # In dollars; "NA"/"Exempt" → NULL
        # Borrower Financials
        "income":                    IntegerType(),   # Annual income in $thousands; "NA" → NULL
        "debt_to_income_ratio":      StringType(),    # Ranges: "<20%", "20%-<30%", "Exempt", "NA"…
        "applicant_credit_score_type": ShortType(),  # 1-9 enum
        "applicant_sex":             ShortType(),     # 1-6 enum
        "applicant_age":             StringType(),    # Ranges: "25-34", "<25", ">74", "NA"…
        # Outcome & Compliance
        "action_taken":              ShortType(),     # 1=Originated, 2=Approved not accepted…
        "hoepa_status":              ShortType(),     # 1=HOEPA, 2=Not HOEPA, 3=NA
        "lien_status":               ShortType(),     # 1=First lien, 2=Subordinate lien
    }
    target_columns = list(TARGET_SCHEMA.keys())

    spark = get_spark()
    try:
        print(f"Reading CSV: {local_file}")
        df_raw = spark.read \
                      .option("header", "true") \
                      .option("delimiter", "|") \
                      .csv(f"file://{local_file}")

        df_selected = df_raw.select(*target_columns)
        df_filtered = df_selected.filter(F.col("action_taken") == "1")

        df_final = df_filtered
        for col_name, col_type in TARGET_SCHEMA.items():
            if not isinstance(col_type, StringType):
                df_final = df_final.withColumn(col_name, F.col(col_name).cast(col_type))

        local_parquet_dir = os.path.join(workspace, "hmda_parquet_output")
        print(f"Writing {len(target_columns)} columns to local disk as Parquet...")
        df_final.write.mode("overwrite") \
                .partitionBy("state_code") \
                .parquet(f"file://{local_parquet_dir}")
                
        # Return count for the email report later
        return len(target_columns)
        
    finally:
        spark.stop()

def upload_hmda_s3(**kwargs):
    """
    Task 3: Upload the local Parquet directory to AWS S3.
    """
    import os
    import boto3
    from airflow.models import Variable
    
    conf = kwargs.get('dag_run').conf or {}
    year = conf.get('year', 2024)
    workspace = get_hmda_workspace(year)
    local_parquet_dir = os.path.join(workspace, "hmda_parquet_output")
    
    s3_bucket = Variable.get("s3_bucket_name")
    s3_client = boto3.client("s3")
    s3_prefix = f"raw/hmda_processed/{year}/"

    print(f"Checking existing objects under s3://{s3_bucket}/{s3_prefix}")
    paginator = s3_client.get_paginator("list_objects_v2")
    existing_keys = [
        {"Key": obj["Key"]}
        for page in paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)
        for obj in page.get("Contents", [])
    ]
    if existing_keys:
        print(f"Deleting {len(existing_keys)} existing objects...")
        for i in range(0, len(existing_keys), 1000):
            s3_client.delete_objects(
                Bucket=s3_bucket,
                Delete={"Objects": existing_keys[i:i + 1000]}
            )

    print("Uploading partitioned Parquet files to S3 via boto3...")
    uploaded = 0
    for root, dirs, files in os.walk(local_parquet_dir):
        for fname in files:
            local_path = os.path.join(root, fname)
            rel_path = os.path.relpath(local_path, local_parquet_dir)
            s3_key = s3_prefix + rel_path.replace("\\", "/")
            s3_client.upload_file(local_path, s3_bucket, s3_key)
            uploaded += 1

    print(f"Uploaded {uploaded} files to s3://{s3_bucket}/{s3_prefix}")
    
    ti = kwargs['ti']
    col_count = ti.xcom_pull(task_ids='process_hmda_spark') or 20
    
    send_status_email("SUCCESS", "HMDA_Ingestion_Modular", year,
                      f"Ingested {col_count} columns, {uploaded} partitioned Parquet files uploaded to S3.")

def cleanup_hmda_local(**kwargs):
    """
    Task 4: Cleanup local working directory to free up disk space.
    Runs even if prior tasks failed.
    """
    import os
    import shutil
    
    conf = kwargs.get('dag_run').conf or {}
    year = conf.get('year') or kwargs.get('logical_date').year
    workspace = get_hmda_workspace(year)
    
    if os.path.exists(workspace):
        print(f"Removing local workspace: {workspace}")
        shutil.rmtree(workspace)
        print("Cleanup complete.")
    else:
        print(f"Workspace {workspace} doesn't exist. Nothing to clean.")

# --- DAG Definition ---
with DAG(
    'credit_risk_master_ingestion',
    default_args=default_args,
    description='Master Ingestion: FRED API & Modular HMDA Spark on LocalExecutor',
    schedule_interval='@yearly',
    catchup=False,
    params={
        "year": 2024
    }
) as dag:

    # Independent FRED ingestion task
    task_fred = PythonOperator(
        task_id='ingest_fred_api',
        python_callable=ingest_fred_to_raw
    )

    # Modular HMDA pipeline tasks
    task_dl_hmda = PythonOperator(
        task_id='download_hmda_data',
        python_callable=download_hmda_data,
        provide_context=True
    )

    task_spark_hmda = PythonOperator(
        task_id='process_hmda_spark',
        python_callable=process_hmda_spark,
        provide_context=True
    )

    task_s3_hmda = PythonOperator(
        task_id='upload_hmda_s3',
        python_callable=upload_hmda_s3,
        provide_context=True
    )
    
    task_clean_hmda = PythonOperator(
        task_id='cleanup_hmda_local',
        python_callable=cleanup_hmda_local,
        provide_context=True,
        trigger_rule='all_done' # Ensure cleanup runs even if upload or process fails
    )

    # Define execution graph
    # FRED drops directly into S3 independently
    task_fred
    
    # HMDA runs sequentially to minimize concurrent RAM usage and manage data handoff
    task_dl_hmda >> task_spark_hmda >> task_s3_hmda >> task_clean_hmda