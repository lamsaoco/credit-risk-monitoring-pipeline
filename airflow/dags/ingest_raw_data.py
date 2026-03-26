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

# --- HMDA Spark Task ---
def ingest_hmda_raw_spark(**kwargs):
    """
    High-performance Spark ETL for HMDA dataset on m7i-flex.large (8GB RAM).
    Processes 20 specific columns with an explicit schema to skip inferSchema overhead.
    """
    import os
    import sys
    from pyspark.sql.types import ShortType, IntegerType, DoubleType, StringType
    from pyspark.sql import functions as F
    import requests
    import zipfile

    # Ensure Spark uses the correct Python interpreter from .venv
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    s3_bucket = Variable.get("s3_bucket_name")
    year = kwargs.get('dag_run').conf.get('year', 2024)
    spark = get_spark()

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

    try:
        # 2. DATA ACQUISITION
        os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
        download_url = f"https://files.ffiec.cfpb.gov/dynamic-data/{year}/{year}_lar.zip"
        zip_path = os.path.join(LOCAL_TMP_DIR, f"hmda_{year}.zip")
        
        print(f"Downloading HMDA {year} dataset...")
        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            with open(zip_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(LOCAL_TMP_DIR)
            local_file = os.path.join(LOCAL_TMP_DIR, zip_ref.namelist()[0])

        # 3. SPARK PROCESSING
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


        # 4. WRITE PARQUET LOCALLY, THEN UPLOAD TO S3 VIA BOTO3
        local_parquet_dir = os.path.join(LOCAL_TMP_DIR, "hmda_parquet_output")
        print(f"Writing {len(target_columns)} columns to local disk as Parquet...")
        df_final.write.mode("overwrite") \
                .partitionBy("state_code") \
                .parquet(f"file://{local_parquet_dir}")

        print("Uploading partitioned Parquet files to S3 via boto3...")
        import boto3
        s3_client = boto3.client("s3")
        s3_prefix = f"raw/hmda_processed/{year}/"

        # Delete existing objects under the year prefix before re-uploading
        paginator = s3_client.get_paginator("list_objects_v2")
        existing_keys = [
            {"Key": obj["Key"]}
            for page in paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)
            for obj in page.get("Contents", [])
        ]
        if existing_keys:
            print(f"Deleting {len(existing_keys)} existing objects under s3://{s3_bucket}/{s3_prefix}")
            for i in range(0, len(existing_keys), 1000):
                s3_client.delete_objects(
                    Bucket=s3_bucket,
                    Delete={"Objects": existing_keys[i:i + 1000]}
                )

        uploaded = 0
        for root, dirs, files in os.walk(local_parquet_dir):
            for fname in files:
                local_path = os.path.join(root, fname)
                rel_path = os.path.relpath(local_path, local_parquet_dir)
                s3_key = s3_prefix + rel_path.replace("\\", "/")
                s3_client.upload_file(local_path, s3_bucket, s3_key)
                uploaded += 1

        print(f"Uploaded {uploaded} files to s3://{s3_bucket}/{s3_prefix}")
        send_status_email("SUCCESS", "HMDA_Ingestion", year,
                          f"Ingested {len(target_columns)} columns, {uploaded} Parquet files uploaded to S3.")

    except Exception as e:
        send_status_email("FAILED", "HMDA_Ingestion", year, str(e))
        raise e
    finally:
        # 5. CLEANUP local temp dir and stop Spark
        if os.path.exists(LOCAL_TMP_DIR):
            shutil.rmtree(LOCAL_TMP_DIR)
        spark.stop()

# --- DAG Definition ---
with DAG(
    'credit_risk_master_ingestion',
    default_args=default_args,
    description='Master Ingestion: FRED API & HMDA Spark with 8GB RAM Optimization',
    schedule_interval='@yearly',
    catchup=False
) as dag:

    task_fred = PythonOperator(
        task_id='ingest_fred_api',
        python_callable=ingest_fred_to_raw
    )

    task_hmda = PythonOperator(
        task_id='ingest_hmda_spark',
        python_callable=ingest_hmda_raw_spark,
        provide_context=True
    )

    # Running in parallel to leverage m7i-flex.large resources
    [task_fred, task_hmda]