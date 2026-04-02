"""
DAG #4: dbt_transform_mart
==========================
Triggers dbt build to transform staging data into analytics marts.
Runs after the warehouse load DAG succeeds, OR runs immediately if triggered manually.

Backend switching (controlled entirely by DW_BACKEND in docker/.env):
  DW_BACKEND=postgresql  → dbt target: dev   | Docker service: dbt (dbt-postgres image)
  DW_BACKEND=snowflake   → dbt target: snowflake | Docker service: dbt (dbt-snowflake image)

The Docker Compose 'dbt' service image is driven by DBT_ADAPTER env var. No DAG change needed.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import os

# Path to docker-compose.yml inside the container
DOCKER_COMPOSE_FILE = "/opt/airflow/docker/docker-compose.yml"

# ─────────────────────────────────────────────────────── #
# Backend switch: reads from env var set in docker/.env   #
# Switch backend by changing DW_BACKEND in .env only.     #
# ─────────────────────────────────────────────────────── #
DW_BACKEND  = os.getenv("DW_BACKEND", "postgresql")
DBT_TARGET  = "snowflake" if DW_BACKEND == "snowflake" else "dev"
# Note: Docker service 'dbt' picks its image from DBT_ADAPTER env var in docker-compose.yml

default_args = {
    "owner": "PhanNH",
    "start_date": datetime(2026, 3, 30),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def check_run_mode(**kwargs):
    """
    Branching logic:
    If the DAG is triggered manually (via UI), skip the sensor and run immediately.
    If triggered by a schedule, wait for the upstream warehouse DAG to finish.
    """
    dag_run = kwargs.get("dag_run")
    # In Airflow, run_type can be a string or an enum depending on the version
    if dag_run and getattr(dag_run.run_type, "value", dag_run.run_type) == "manual":
        return "dbt_debug"
    return "wait_for_warehouse_load"

with DAG(
    "credit_risk_dbt_marts",
    default_args=default_args,
    description="Run dbt models to transform staging to marts",
    schedule_interval=None,  # You can change this to a cron expression if needed
    catchup=False,
    tags=["dbt", "warehouse", "marts"],
    params={
        "data_year": Param(
            2024, 
            type="integer", 
            title="Data Year",
            description="Filter data by year for separate dbt processing (when run manually)"
        ),
    },
) as dag:

    # 0. Check how the DAG was triggered
    branch_task = BranchPythonOperator(
        task_id="check_run_mode",
        python_callable=check_run_mode,
    )

    # 1. Wait for completion of Task 3.4 (load_to_warehouse)
    wait_for_warehouse_load = ExternalTaskSensor(
        task_id="wait_for_warehouse_load",
        external_dag_id="credit_risk_load_warehouse",
        external_task_id="load_to_warehouse",
        check_existence=True,
        timeout=600,
        mode="reschedule",
    )

    # 2. dbt debug: Verify connection (uses --target to point at correct DW)
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"docker compose -f {DOCKER_COMPOSE_FILE} run --rm dbt debug --target {DBT_TARGET}",
        trigger_rule=TriggerRule.NONE_FAILED,  # Allows running if `wait_for_warehouse_load` is skipped
    )

    # 3. dbt seed: Load lookup CSVs
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"docker compose -f {DOCKER_COMPOSE_FILE} run --rm dbt seed --target {DBT_TARGET}",
    )

    # 4. dbt deps: Install any dbt packages (if any)
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"docker compose -f {DOCKER_COMPOSE_FILE} run --rm dbt deps --target {DBT_TARGET}",
    )

    # 5. dbt build: Run models and tests with year parameter
    # --target selects the correct database (dev=PostgreSQL, snowflake=Snowflake)
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"docker compose -f {DOCKER_COMPOSE_FILE} run --rm dbt build"
            f" --target {DBT_TARGET}"
            " --vars 'data_year: {{ params.data_year }}'"
        ),
    )

    # Define the DAG flow
    branch_task >> wait_for_warehouse_load >> dbt_debug
    branch_task >> dbt_debug
    
    dbt_debug >> dbt_seed >> dbt_deps >> dbt_build
