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
    Higher memory allocation for driver to handle large metadata efficiently.
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
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.network.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10") \
        .config("spark.hadoop.fs.s3a.paging.maximum", "5000") \
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
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
    from pyspark.sql import functions as F
    import requests
    import zipfile

    # Ensure Spark uses the correct Python interpreter from .venv
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    s3_bucket = Variable.get("s3_bucket_name")
    year = kwargs.get('dag_run').conf.get('year', 2024)
    spark = get_spark()

    # --- 1. DEFINE EXPLICIT SCHEMA (20 COLUMNS) ---
    # Defining types upfront saves significant RAM and prevents I/O hanging
    hmda_schema = StructType([
        # 1-5: Identification & Geography
        StructField("activity_year", IntegerType(), True),
        StructField("lei", StringType(), True),
        StructField("state_code", StringType(), True),
        StructField("county_code", StringType(), True),
        StructField("census_tract", StringType(), True),
        
        # 6-12: Loan Details
        StructField("loan_type", IntegerType(), True),
        StructField("loan_purpose", IntegerType(), True),
        StructField("loan_amount", DoubleType(), True),
        StructField("interest_rate", DoubleType(), True),
        StructField("property_value", DoubleType(), True),
        StructField("occupancy_type", IntegerType(), True),
        StructField("total_loan_costs", DoubleType(), True),
        
        # 13-17: Borrower Financials
        StructField("income", DoubleType(), True),
        StructField("debt_to_income_ratio", StringType(), True), # Handled as string for ranges like '<20%'
        StructField("applicant_credit_score_type", IntegerType(), True),
        StructField("applicant_sex", IntegerType(), True),
        StructField("applicant_age", StringType(), True),
        
        # 18-20: Result & Compliance
        StructField("action_taken", IntegerType(), True),
        StructField("hoepa_status", IntegerType(), True),
        StructField("lien_status", IntegerType(), True)
    ])

    # List of columns to select for the final Parquet file
    target_columns = [field.name for field in hmda_schema]

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
        # Read with defined schema and pipe delimiter
        df = spark.read.options(header='True', delimiter='|') \
                  .schema(hmda_schema) \
                  .csv(f"file://{local_file}")
        
        # Explicit Selection: Only keep the 20 columns defined in schema
        # Filtering for successful originations (action_taken = 1)
        df_final = df.select(*target_columns).filter(F.col("action_taken") == 1)

        # 4. WRITE TO S3 (RAW ZONE)
        # Partitioning by state_code is optimal for downstream regional analysis
        s3_output = f"s3a://{s3_bucket}/raw/hmda_processed/{year}/"
        print(f"Writing {len(target_columns)} columns to S3 as Parquet...")
        
        df_final.write.mode("overwrite") \
                .partitionBy("state_code") \
                .parquet(s3_output)
        
        send_status_email("SUCCESS", "HMDA_Ingestion", year, 
                          f"Successfully ingested {len(target_columns)} columns into Raw Zone.")

    except Exception as e:
        send_status_email("FAILED", "HMDA_Ingestion", year, str(e))
        raise e
    finally:
        # 5. CLEANUP
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