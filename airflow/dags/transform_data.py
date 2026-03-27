from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.email import send_email
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import sys

# --- Professional Failure Notification ---
def notify_pipeline_failure(context):
    """
    Standard incident reporting for Mortgage Pipeline.
    Triggered automatically on task failure.
    """
    ti = context.get('task_instance')
    receiver = Variable.get("email_receiver")
    subject = f"⚠️ [INCIDENT] Mortgage Silver Enrichment Failed: {ti.task_id}"
    
    html_content = f"""
    <h3>Mortgage Data Pipeline - Failure Alert</h3>
    <p><b>DAG ID:</b> {ti.dag_id}</p>
    <p><b>Task ID:</b> {ti.task_id}</p>
    <p><b>Logical Date:</b> {context.get('logical_date')}</p>
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

# --- TASK 2: ENRICHMENT & FEATURE ENGINEERING ---
def enrich_mortgage_features_spark(**kwargs):
    """
    Core Processing: Performs Broadcast Join with Market Rates and calculates LTV/DTI.
    """
    from pyspark.sql import SparkSession, functions as F

    # Dynamic year logic as requested
    conf = kwargs.get('dag_run').conf or {}
    year = conf.get('year') or kwargs.get('logical_date').year
    bucket = Variable.get("s3_bucket_name")

    # Spark Optimization for m7i-flex.large (8GB RAM)
    os.environ['PYSPARK_PYTHON'] = sys.executable
    spark = SparkSession.builder \
        .appName(f"Mortgage_Silver_Enrichment_{year}") \
        .config("spark.driver.memory", "4g") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .getOrCreate()

    try:
        print(f"🚀 Processing Enrichment for Mortgage Year: {year}")

        # A. EXTRACTION (In-Memory from Raw Zone)
        df_hmda = spark.read.parquet(f"s3a://{bucket}/raw/hmda_processed/{year}/")
        df_fred = spark.read.parquet(f"s3a://{bucket}/raw/fred_raw/data.parquet")

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
            "debt_to_income_numeric", 
            F.regexp_extract(F.col("debt_to_income_ratio"), r"(\d+)", 1).cast("double") / 100
        )

        # D. BROADCAST JOIN & SPREAD ANALYSIS (Task 3.1)
        # Using Broadcast Join for the small FRED table to optimize 12M row processing
        silver_layer = mortgage_enriched.join(
            F.broadcast(fred_benchmark),
            mortgage_enriched.activity_year == fred_benchmark.yr,
            "left"
        ).withColumn("interest_rate_spread", F.col("interest_rate") - F.col("mkt_benchmark"))

        # E. PERSISTENCE (Writing to Silver/Curated Zone)
        output_path = f"s3a://{bucket}/silver/mortgage_curated/{year}/"
        silver_layer.write.mode("overwrite") \
                    .partitionBy("state_code") \
                    .parquet(output_path)
        
        print(f"✅ Curated data successfully persisted to: {output_path}")

    finally:
        # Crucial for 8GB RAM instances to release memory
        spark.stop()

# --- DAG Orchestration ---
with DAG(
    'mortgage_risk_silver_enrichment',
    default_args=default_args,
    description='Enriches 12M Mortgage records with Market Rates and Risk Ratios',
    schedule_interval='@yearly', # Automatically runs each year
    catchup=False,
    tags=['mortgage', 'silver', 'spark']
) as dag:

    t1 = PythonOperator(
        task_id='validate_raw_mortgage_landing',
        python_callable=validate_raw_mortgage_landing,
        provide_context=True
    )

    t2 = PythonOperator(
        task_id='enrich_mortgage_features_spark',
        python_callable=enrich_mortgage_features_spark,
        provide_context=True
    )

    t1 >> t2