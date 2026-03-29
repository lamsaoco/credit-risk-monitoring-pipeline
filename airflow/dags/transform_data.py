from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.email import send_email
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.models.param import Param
import os
import sys
import shutil
import boto3

def get_staging_workspace(year):
    return f"/tmp/mortgage_staging_workspace_{year}"

# --- Professional Failure Notification ---
def notify_pipeline_failure(context):
    """
    Standard incident reporting for Mortgage Pipeline.
    Triggered automatically on task failure.
    """
    ti = context.get('task_instance')
    exception = context.get('exception')
    
    receiver = Variable.get("email_receiver")
    subject = f"⚠️ [INCIDENT] Mortgage Staging Enrichment Failed: {ti.task_id}"
    
    html_content = f"""
    <h3>Mortgage Data Pipeline - Failure Alert</h3>
    <p><b>DAG ID:</b> {ti.dag_id}</p>
    <p><b>Task ID:</b> {ti.task_id}</p>
    <p><b>Logical Date:</b> {context.get('logical_date')}</p>
    <p style="color:darkred; border-left: 4px solid red; padding-left: 10px;"><b>Error Details:</b><br/>{str(exception)}</p>
    <p><b>Log URL:</b> <a href="{ti.log_url}">Review Technical Logs</a></p>
    <p style="color:red;"><i>Note: Check EC2 RAM usage if Spark failed.</i></p>
    """
    send_email(to=receiver, subject=subject, html_content=html_content)

default_args = {
    'owner': 'PhanNH',
    'start_date': datetime(2026, 3, 20), # Start date in the past for catchup testing
    'retries': 0,
    'on_failure_callback': notify_pipeline_failure,
}

# --- TASK 1: LANDING ZONE VALIDATION ---
def validate_raw_mortgage_landing(**kwargs):
    """
    Business Check: Ensures HMDA Raw data for the target year has landed on S3.
    """
    # Dynamic year logic as requested
    conf = kwargs.get('dag_run').conf or {}
    year = conf.get('year') or kwargs.get('logical_date').year
    
    bucket = Variable.get("s3_bucket_name")
    prefix = f"raw/hmda_processed/{year}/"
    
    hook = S3Hook(aws_conn_id='aws_default')
    if not hook.list_keys(bucket, prefix=prefix, max_items=1):
        raise FileNotFoundError(f"Data Integrity Error: Raw HMDA folder for year {year} not found at {prefix}.")
    print(f"✅ Landing Zone Validation Successful for year {year}.")

# --- TASK 2: DOWNLOAD RAW TO LOCAL (Bypassing hadoop-aws bug) ---
def download_raw_to_local_boto3(**kwargs):
    conf = kwargs.get('dag_run').conf or {}
    year = conf.get('year') or kwargs.get('logical_date').year
    bucket = Variable.get("s3_bucket_name")
    workspace = get_staging_workspace(year)
    
    s3 = boto3.client('s3')
    
    # Download hmda and fred prefixes
    hmda_prefix = f"raw/hmda_processed/{year}/"
    fred_prefix = "raw/fred_raw/"
    
    for prefix in [hmda_prefix, fred_prefix]:
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('/'): continue
                
                local_path = os.path.join(workspace, key)
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                s3.download_file(bucket, key, local_path)
    print(f"✅ Downloaded Raw Data to Local Workspace: {workspace}")

# --- TASK 3: ENRICHMENT & FEATURE ENGINEERING (Local Spark) ---
def enrich_mortgage_features_spark(**kwargs):
    """
    Core Processing: Performs Broadcast Join with Market Rates and calculates LTV/DTI.
    """
    from pyspark.sql import SparkSession, functions as F

    # Dynamic year logic as requested
    conf = kwargs.get('dag_run').conf or {}
    year = conf.get('year') or kwargs.get('logical_date').year
    workspace = get_staging_workspace(year)

    # Spark Optimization for m7i-flex.large (8GB RAM)
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    spark = SparkSession.builder \
        .appName(f"Mortgage_Staging_Enrichment_{year}") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    try:
        print(f"🚀 Processing Enrichment LOCALLY for Mortgage Year: {year}")

        # A. EXTRACTION (Local Disk instead of S3A)
        hmda_local_path = os.path.join(workspace, f"raw/hmda_processed/{year}/")
        fred_local_path = os.path.join(workspace, "raw/fred_raw/")
        
        df_hmda = spark.read.parquet(f"file://{hmda_local_path}")
        df_fred = spark.read.parquet(f"file://{fred_local_path}")

        # B. MARKET BENCHMARK AGGREGATION
        # Calculating average annual market rate from FRED indices
        fred_benchmark = df_fred.withColumn("yr", F.year("observation_date")) \
                               .groupBy("yr").agg(F.avg("fed_rate").alias("mkt_benchmark"))

        # C. RISK FEATURE ENGINEERING (Task 3.2)
        # 1. Loan-to-Value (LTV) Ratio: Primary credit risk indicator
        # 2. Debt-to-Income (DTI) Sanitization: Regex to parse numeric value from '36%-45%' strings
        mortgage_enriched = df_hmda.withColumn(
            "loan_to_value_ratio", F.col("loan_amount") / F.col("property_value")
        ).withColumn(
            "extracted_dti",
            F.regexp_extract(F.col("debt_to_income_ratio"), r"(\d+)", 1)
        ).withColumn(
            "debt_to_income_numeric", 
            F.when(F.col("extracted_dti") == "", F.lit(None))
             .otherwise(F.col("extracted_dti").cast("double") / 100)
        ).drop("extracted_dti")

        # D. BROADCAST JOIN & SPREAD ANALYSIS (Task 3.1)
        # Using Broadcast Join for the small FRED table to optimize 12M row processing
        staging_layer = mortgage_enriched.join(
            F.broadcast(fred_benchmark),
            mortgage_enriched.activity_year == fred_benchmark.yr,
            "left"
        ).withColumn("interest_rate_spread", F.col("interest_rate") - F.col("mkt_benchmark"))

        # E. PERSISTENCE (Local Disk)
        output_path = os.path.join(workspace, f"staging/mortgage_curated/{year}")
        staging_layer.write.mode("overwrite") \
                    .partitionBy("state_code") \
                    .parquet(f"file://{output_path}")
        
        print(f"✅ Curated data locally persisted to: {output_path}")

    finally:
        spark.stop()

# --- TASK 4: UPLOAD ENRICHED DATA TO S3 ---
def upload_staging_to_s3(**kwargs):
    conf = kwargs.get('dag_run').conf or {}
    year = conf.get('year') or kwargs.get('logical_date').year
    bucket = Variable.get("s3_bucket_name")
    workspace = get_staging_workspace(year)
    
    s3 = boto3.client('s3')
    local_output_dir = os.path.join(workspace, f"staging/mortgage_curated/{year}")
    s3_prefix = f"staging/mortgage_curated/{year}/"
    
    # 1. Delete existing S3 objects in staging dir
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=s3_prefix):
        if 'Contents' in page:
            objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
            s3.delete_objects(Bucket=bucket, Delete={'Objects': objects_to_delete})
            
    # 2. Upload locally written parquet files
    for root, dirs, files in os.walk(local_output_dir):
        for file in files:
            local_path = os.path.join(root, file)
            # Preserve partition structure
            relative_path = os.path.relpath(local_path, local_output_dir)
            s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")
            
            s3.upload_file(local_path, bucket, s3_key)
            print(f"Uploaded {s3_key} to S3")

# --- TASK 5: LOCAL CLEANUP ---
def cleanup_staging_local(**kwargs):
    conf = kwargs.get('dag_run').conf or {}
    year = conf.get('year') or kwargs.get('logical_date').year
    workspace = get_staging_workspace(year)
    
    if os.path.exists(workspace):
        shutil.rmtree(workspace)
        print(f"✅ Cleaned up local workspace: {workspace}")

# --- DAG Orchestration ---
with DAG(
    'mortgage_risk_staging_enrichment',
    default_args=default_args,
    description='Enriches 12M Mortgage records with Market Rates and Risk Ratios',
    schedule_interval='@yearly', # Automatically runs each year
    catchup=False,
    tags=['mortgage', 'staging', 'spark'],
    params={
        "year": Param(2024, type="integer", title="Processing Year")
    }
) as dag:

    t1 = PythonOperator(
        task_id='validate_raw_mortgage_landing',
        python_callable=validate_raw_mortgage_landing,
        provide_context=True
    )

    t2 = PythonOperator(
        task_id='download_raw_to_local_boto3',
        python_callable=download_raw_to_local_boto3,
        provide_context=True
    )

    t3 = PythonOperator(
        task_id='enrich_mortgage_features_spark',
        python_callable=enrich_mortgage_features_spark,
        provide_context=True
    )

    t4 = PythonOperator(
        task_id='upload_staging_to_s3',
        python_callable=upload_staging_to_s3,
        provide_context=True
    )

    t5 = PythonOperator(
        task_id='cleanup_staging_local',
        python_callable=cleanup_staging_local,
        provide_context=True,
        trigger_rule="all_done"
    )

    t1 >> t2 >> t3 >> t4 >> t5